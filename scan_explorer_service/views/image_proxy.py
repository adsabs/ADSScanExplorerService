from typing import Union
from flask import Blueprint, Response, current_app, request, stream_with_context, jsonify
from flask_discoverer import advertise
from urllib import parse as urlparse
import img2pdf
import math
import requests
from scan_explorer_service.models import Collection, Page, Article
from scan_explorer_service.utils.db_utils import item_thumbnail
from scan_explorer_service.utils.s3_utils import S3Provider
from scan_explorer_service.utils.utils import url_for_proxy

bp_proxy = Blueprint('proxy', __name__, url_prefix='/image')


@advertise(scopes=['api'], rate_limit=[5000, 3600*24])
@bp_proxy.route('/iiif/2/<path:path>', methods=['GET'])
def image_proxy(path):
    """Proxy in between the image server and the user"""
    req_url = urlparse.urljoin(f'{current_app.config.get("IMAGE_API_BASE_URL")}/', path)
    req_headers = {key: value for (key, value) in request.headers if key != 'Host' and key != 'Accept'}

    req_headers['X-Forwarded-Host'] = current_app.config.get('PROXY_SERVER')
    req_headers['X-Forwarded-Path'] = current_app.config.get('PROXY_PREFIX').rstrip('/') + '/image'
    request_method = request.method
    

    current_app.logger.info(f'Request method: {request_method}')

    r = requests.request(request_method, req_url, params=request.args, stream=True,
                         headers=req_headers, allow_redirects=False, data=request.form)

    excluded_headers = ['content-encoding','content-length', 'transfer-encoding', 'connection']
    headers = [(name, value) for (name, value) in r.headers.items() if name.lower() not in excluded_headers]

    @stream_with_context
    def generate():
        for chunk in r.raw.stream(decode_content=False):
            yield chunk
    return Response(generate(), status=r.status_code, headers=headers)


@advertise(scopes=['api'], rate_limit=[5000, 3600*24])
@bp_proxy.route('/thumbnail', methods=['GET'])
def image_proxy_thumbnail():
    """Helper to generate the correct url for a thumbnail given an ID and type"""
    try:
        id = request.args.get('id')
        type = request.args.get('type')
        with current_app.session_scope() as session:
            thumbnail_path = item_thumbnail(session, id, type)
            path = urlparse.urlparse(thumbnail_path).path
            
            remove = urlparse.urlparse(url_for_proxy('proxy.image_proxy', path='')).path
            path = path.replace(remove, '')
            
            return image_proxy(path)
    except Exception as e:
        current_app.logger.info(f'{e}')
        return jsonify(Message=str(e)), 400
    
def get_item(session, id): 
    item: Union[Article, Collection] = (
                session.query(Article).filter(Article.id == id).one_or_none()
                or session.query(Collection).filter(Collection.id == id).one_or_none())
    if not item: 
        raise Exception("ID: " + id + " not found")
    
    return item 


def get_pages(item, session, page_start, page_end, page_limit): 
    if isinstance(item, Article):
        start_page = item.pages.first().volume_running_page_num
        query = session.query(Page).filter(Page.articles.any(Article.id == item.id), 
            Page.volume_running_page_num  >= page_start + start_page - 1, 
            Page.volume_running_page_num  <= page_end + start_page - 1).order_by(Page.volume_running_page_num).limit(page_limit)
    elif isinstance(item, Collection):
        query = session.query(Page).filter(Page.collection_id == item.id, 
            Page.volume_running_page_num >= page_start, 
            Page.volume_running_page_num <= page_end).order_by(Page.volume_running_page_num).limit(page_limit)
    return query 


@stream_with_context
def fetch_images(session, item, page_start, page_end, page_limit, scaling, dpi, memory_limit):
        n_pages = 0
        memory_sum = 0
        query = get_pages(item, session, page_start, page_end, page_limit)
        
        for page in query.all():
            
            n_pages += 1
            
            current_app.logger.info(f"Generating image for page: {n_pages}") 
            current_app.logger.info(f'Id: {page.id}, Volume_page: {page.volume_running_page_num}, memory: {memory_sum}')
            if n_pages > page_limit:
                break
            if memory_sum > memory_limit:
                current_app.logger.error(f"Memory limit reached: {memory_sum} > {memory_limit}") 
                break
            size = 'full'
            if dpi != 600:
                size = str(int(page.width*scaling))+ ","
            image_url = page.image_url + "/full/" + size + f"/0/{page.image_color_quality}.tif"
            current_app.logger.info(f"image url: {page.image_url}") 
            path = urlparse.urlparse(image_url).path
            current_app.logger.info(f"path: {path}") 
            remove = urlparse.urlparse(url_for_proxy('proxy.image_proxy', path='')).path
            current_app.logger.info(f"remove: {remove}") 
            path = path.replace(remove, '')
            current_app.logger.info(f"modified path: {path}") 
            im_data = image_proxy(path).get_data()
            current_app.logger.info(f"Getting image data...: {im_data}") 
            yield im_data

def fetch_pdf(object_name):

    full_path = f'pdfs/{object_name}'
    file_content = S3Provider(current_app.config).read_object_s3(full_path)
    current_app.logger.info(f"Successfully fetched PDF from S3 bucket: {object_name}")
    response = Response(file_content, mimetype='application/pdf')
    response.headers['Content-Disposition'] = f'attachment; filename="{object_name}"'

    return response

@advertise(scopes=['api'], rate_limit=[5000, 3600*24])
@bp_proxy.route('/pdf', methods=['GET'])
def pdf_save():
    """Generate a PDF from pages"""
    
    try:
        id = request.args.get('id')
        page_start = request.args.get('page_start', 1, int)
        page_end = request.args.get('page_end', math.inf, int)
        dpi = request.args.get('dpi', 600, int)
        dpi = min(dpi, 600)
        scaling = float(dpi)/ 600
        memory_limit = current_app.config.get("IMAGE_PDF_MEMORY_LIMIT")
        page_limit = current_app.config.get("IMAGE_PDF_PAGE_LIMIT")
        
        current_app.logger.info(f'Memory limit: {memory_limit}')

        with current_app.session_scope() as session:

            item = get_item(session, id) 
            current_app.logger.info(f"Item retrieved successfully: {item.id}")
           
            if isinstance(item, Article): 
                current_app.logger.info(f"Item is an article: {item.id}")
                object_name = f'{item.id}.pdf'.lower()
                try: 
                    response = fetch_pdf(object_name)
                except Exception as e: 
                    current_app.logger.info(f"Failed to get PDF using fallback method for {object_name}: {str(e)}")
                    response = Response(img2pdf.convert([im for im in fetch_images(session, item, page_start, page_end, page_limit, scaling, dpi, memory_limit)]), mimetype='application/pdf') 
            else: 
                current_app.logger.info(f"Attempting to fetch PDF using cantaloupe: {item.id}")
                response = Response(img2pdf.convert([im for im in fetch_images(session, item, page_start, page_end, page_limit, scaling, dpi, memory_limit)]), mimetype='application/pdf') 
            
          
            return response
    except Exception as e:
        return jsonify(Message=str(e)), 400
