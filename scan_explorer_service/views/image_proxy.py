from typing import Union
from flask import Blueprint, Response, current_app, request, stream_with_context, jsonify, send_file
from flask_discoverer import advertise
from urllib import parse as urlparse
import img2pdf
import math
import requests
from scan_explorer_service.models import Collection, Page, Article
from scan_explorer_service.utils.db_utils import item_thumbnail
from scan_explorer_service.utils.s3_utils import S3Provider
from scan_explorer_service.utils.utils import url_for_proxy
import re
import io
import sys
import time

try:
    from gevent.pool import Pool as GeventPool
except ImportError:
    GeventPool = None

bp_proxy = Blueprint('proxy', __name__, url_prefix='/image')


@advertise(scopes=['api'], rate_limit=[5000, 3600*24])
@bp_proxy.route('/iiif/2/<path:path>', methods=['GET'])
def image_proxy(path):
    """Proxy in between the image server and the user"""
    req_url = urlparse.urljoin(f'{current_app.config.get("IMAGE_API_BASE_URL")}/', path)
    req_headers = {key: value for (key, value) in request.headers if key != 'Host' and key != 'Accept'}

    req_headers['X-Forwarded-Host'] = current_app.config.get('PROXY_SERVER')
    req_headers['X-Forwarded-Path'] = current_app.config.get('PROXY_PREFIX').rstrip('/') + '/image'

    encoded_url = re.sub(r"[+&]", "%2B", req_url)

    retries = current_app.config.get('IMAGE_PROXY_RETRIES', 1)
    retry_delay = current_app.config.get('IMAGE_PROXY_RETRY_DELAY', 2)

    r = requests.request(request.method, encoded_url, params=request.args, stream=True,
                         headers=req_headers, allow_redirects=False, data=request.form)

    for attempt in range(retries):
        if r.status_code < 400:
            break
        current_app.logger.warning(
            f"Upstream image request failed (status {r.status_code}), "
            f"retrying in {retry_delay}s (attempt {attempt + 1}/{retries})")
        time.sleep(retry_delay)
        r.close()
        r = requests.request(request.method, encoded_url, params=request.args, stream=True,
                             headers=req_headers, allow_redirects=False, data=request.form)

    excluded_headers = ['content-encoding','content-length', 'transfer-encoding', 'connection']
    headers = [(name, value) for (name, value) in r.headers.items() if name.lower() not in excluded_headers]

    @stream_with_context
    def generate():
        for chunk in r.raw.stream(decode_content=False):
            yield chunk

    resp = Response(generate(), status=r.status_code, headers=headers)
    resp.call_on_close(r.close)
    return resp


@advertise(scopes=['api'], rate_limit=[5000, 3600*24])
@bp_proxy.route('/thumbnail', methods=['GET'])
def image_proxy_thumbnail():
    """Helper to generate the correct url for a thumbnail given an ID and type"""
    try:
        id = request.args.get('id').replace(" ", "+")
        type = request.args.get('type')
        with current_app.session_scope() as session:
            thumbnail_path = item_thumbnail(session, id, type)
            path = urlparse.urlparse(thumbnail_path).path

            remove = urlparse.urlparse(url_for_proxy('proxy.image_proxy', path='')).path

            path = path.replace(remove, '')

            return image_proxy(path)
    except Exception as e:
        current_app.logger.exception(f'{e}')
        return jsonify(Message=str(e)), 400

def get_item(session, id):
    item: Union[Article, Collection] = (
                session.query(Article).filter(Article.id == id).one_or_none()
                or session.query(Collection).filter(Collection.id == id).one_or_none())
    if not item:
        raise Exception("ID: " + str(id) + " not found")

    return item


def get_pages(item, session, page_start, page_end, page_limit):
    if isinstance(item, Article):
        first_page = item.pages.first()
        if first_page is None:
            raise Exception(f"No pages found for article {item.id}")
        start_page = first_page.volume_running_page_num
        query = session.query(Page).filter(Page.articles.any(Article.id == item.id),
            Page.volume_running_page_num  >= page_start + start_page - 1,
            Page.volume_running_page_num  <= page_end + start_page - 1).order_by(Page.volume_running_page_num).limit(page_limit)
    elif isinstance(item, Collection):
        query = session.query(Page).filter(Page.collection_id == item.id,
            Page.volume_running_page_num >= page_start,
            Page.volume_running_page_num <= page_end).order_by(Page.volume_running_page_num).limit(page_limit)
    return query


def fetch_images(session, item, page_start, page_end, page_limit, memory_limit):
    query = get_pages(item, session, page_start, page_end, page_limit)
    pages = query.all()

    page_objects = []
    for page in pages[:page_limit]:
        image_path, fmt = page.image_path_basic
        object_name = '/'.join(image_path) + fmt
        page_objects.append(object_name)

    config = current_app.config
    s3 = S3Provider(config, 'AWS_BUCKET_NAME_IMAGE')

    def _fetch(obj_name):
        return s3.read_object_s3(obj_name)

    pool = None
    if GeventPool is not None:
        pool = GeventPool(size=20)
        results = pool.imap(_fetch, page_objects)
    else:
        results = (_fetch(obj) for obj in page_objects)

    try:
        memory_sum = 0
        for im_data in results:
            if not im_data:
                continue
            memory_sum += sys.getsizeof(im_data)
            if memory_sum > memory_limit:
                current_app.logger.error(f"Memory limit reached: {memory_sum} > {memory_limit}")
                break
            yield im_data
    finally:
        if pool is not None:
            pool.kill()


def fetch_object(object_name, bucket_name):
    file_content = S3Provider(current_app.config, bucket_name).read_object_s3(object_name)
    if not file_content:
        current_app.logger.error(f"Failed to fetch content for {object_name}. File might be empty.")
        raise ValueError(f"File content is empty for {object_name}")
    return file_content


def fetch_article(item, memory_limit):
    object_name = f'{item.id}.pdf'.lower()
    try:
        full_path = f'pdfs/{object_name}'
        file_content = fetch_object(full_path, 'AWS_BUCKET_NAME_PDF')

        if len(file_content) > memory_limit:
            current_app.logger.error(f"Memory limit reached: {len(file_content)} > {memory_limit}")

        file_stream = io.BytesIO(file_content)
        file_stream.seek(0)
        return send_file(
            file_stream,
            as_attachment=True,
            attachment_filename=object_name,
            mimetype='application/pdf'
        )
    except Exception as e:
        current_app.logger.exception(f"Failed to get PDF for {object_name}: {str(e)}")


def generate_pdf(item, session, page_start, page_end, page_limit, memory_limit):
    if isinstance(item, Article):
        response = fetch_article(item, memory_limit)
        if response:
            return response
        else:
            current_app.logger.debug(f"Response fetch article was empty")
            page_end = page_limit
    current_app.logger.debug(f"Article is not an article or fetch article failed.")
    return Response(img2pdf.convert([im for im in fetch_images(session, item, page_start, page_end, page_limit, memory_limit)]), mimetype='application/pdf')


@advertise(scopes=['api'], rate_limit=[5000, 3600*24])
@bp_proxy.route('/pdf', methods=['GET'])
def pdf_save():
    """Generate a PDF from pages"""
    try:
        id = request.args.get('id')
        page_start = request.args.get('page_start', 1, int)
        page_end = request.args.get('page_end', math.inf, int)
        memory_limit = current_app.config.get("IMAGE_PDF_MEMORY_LIMIT")
        page_limit = current_app.config.get("IMAGE_PDF_PAGE_LIMIT")

        if page_end != math.inf and (page_end - page_start + 1) > page_limit:
            return jsonify(Message=f"Requested {page_end - page_start + 1} pages exceeds limit of {page_limit}"), 400

        if not id:
            return jsonify(Message="Missing required parameter: id"), 400

        with current_app.session_scope() as session:

            item = get_item(session, id)
            current_app.logger.debug(f"Item retrieved successfully: {item.id}")

            response = generate_pdf(item, session, page_start, page_end, page_limit, memory_limit)
            return response
    except Exception as e:
        return jsonify(Message=str(e)), 400
