from typing import Union
from flask import Blueprint, Response, current_app, request, stream_with_context, jsonify
from flask_discoverer import advertise
from urllib import parse as urlparse
import img2pdf
import math
import sys
import requests
from scan_explorer_service.models import Collection, Page, Article
from scan_explorer_service.utils.db_utils import item_thumbnail
from scan_explorer_service.utils.utils import url_for_proxy
import io
import cProfile
import pstats


bp_proxy = Blueprint('proxy', __name__, url_prefix='/image')


@advertise(scopes=['api'], rate_limit=[5000, 3600*24])
@bp_proxy.route('/iiif/2/<path:path>', methods=['GET'])
def image_proxy(path):
    """Proxy in between the image server and the user"""
    req_url = urlparse.urljoin(f'{current_app.config.get("IMAGE_API_BASE_URL")}/', path)
    req_headers = {key: value for (key, value) in request.headers if key != 'Host' and key != 'Accept'}

    req_headers['X-Forwarded-Host'] = current_app.config.get('PROXY_SERVER')
    req_headers['X-Forwarded-Path'] = current_app.config.get('PROXY_PREFIX').rstrip('/') + '/image'

    r = requests.request(request.method, req_url, params=request.args, stream=True,
                         headers=req_headers, allow_redirects=False, data=request.form)

    excluded_headers = ['content-encoding','content-length', 'transfer-encoding', 'connection']
    headers = [(name, value) for (name, value) in r.headers.items() if name.lower() not in excluded_headers]

    @stream_with_context
    def generate():
        for chunk in r.raw.stream(decode_content=False):
            yield chunk
    current_app.logger.debug(f'######## Ending image proxy for image {path} ########')
    return Response(generate(), status=r.status_code, headers=headers)


@advertise(scopes=['api'], rate_limit=[5000, 3600*24])
@bp_proxy.route('/thumbnail', methods=['GET'])
def image_proxy_thumbnail():
    """Helper to generate the correct url for a thumbnail given an ID and type"""
    try:
        id = request.args.get('id')
        type = request.args.get('type')
        current_app.logger.debug(f"Id {id}, Type {type}")
        with current_app.session_scope() as session:
            thumbnail_path = item_thumbnail(session, id, type)
            current_app.logger.debug(f"Thumbnail path {thumbnail_path}")
            path = urlparse.urlparse(thumbnail_path).path
            
            remove = urlparse.urlparse(url_for_proxy('proxy.image_proxy', path='')).path
            path = path.replace(remove, '')
            current_app.logger.debug(f"Image thumbnail path {path}")
            
            return image_proxy(path)
    except Exception as e:
        current_app.logger.error(f'{e}')
        return jsonify(Message=str(e)), 400

@advertise(scopes=['api'], rate_limit=[5000, 3600*24])
@bp_proxy.route('/pdf', methods=['GET'])
def pdf_save():
    """Generate a PDF from pages"""
    
    try:
        id = request.args.get('id')
        page_start = request.args.get('page_start', 1, int)
        page_end = request.args.get('page_end', math.inf, int)
        dpi = request.args.get('dpi', 600, int)
        dpi = min(dpi,600)
        scaling = float(dpi)/ 600
        memory_limit = current_app.config.get("IMAGE_PDF_MEMORY_LIMIT")
        page_limit = current_app.config.get("IMAGE_PDF_PAGE_LIMIT")
        
        current_app.logger.debug(f'Memory limit: {memory_limit}')

        @stream_with_context
        def loop_images(id, page_start, page_end):
            n_pages = 0
            memory_sum = 0
            with current_app.session_scope() as session:
                item: Union[Article, Collection] = (
                            session.query(Article).filter(Article.id == id).one_or_none()
                            or session.query(Collection).filter(Collection.id == id).one_or_none())
                current_app.logger.debug(f'######## Fetching article/collection {item} from the database ########')
                if isinstance(item, Article):
                    q = session.query(Article).filter(Article.id == item.id).one_or_none()
                    start_page = q.pages.first().volume_running_page_num
                    query = session.query(Page).filter(Page.articles.any(Article.id == item.id), 
                        Page.volume_running_page_num  >= page_start + start_page - 1, 
                        Page.volume_running_page_num  <= page_end + start_page - 1).order_by(Page.volume_running_page_num).limit(page_limit)
                elif isinstance(item, Collection):
                    query = session.query(Page).filter(Page.collection_id == item.id, 
                        Page.volume_running_page_num >= page_start, 
                        Page.volume_running_page_num <= page_end).order_by(Page.volume_running_page_num).limit(page_limit)
                else:
                    raise Exception("ID: " + id + " not found")
                
                for page in query.all():
                    n_pages += 1
                    if n_pages > page_limit:
                        break
                    if memory_sum > memory_limit:
                        break
                    size = 'full'
                    if dpi != 600:
                        size = str(int(page.width*scaling))+ ","
                    image_url = page.image_url + "/full/" + size + f"/0/{page.image_color_quality}.tif"
                    path = urlparse.urlparse(image_url).path
                    remove = urlparse.urlparse(url_for_proxy('proxy.image_proxy', path='')).path
                    path = path.replace(remove, '')
                    im_data = image_proxy(path).get_data()
                    memory_sum += sys.getsizeof(im_data)
                    current_app.logger.debug(f'Page {n_pages}. Image data: {im_data}')
                    yield im_data
                
                
        profiler = cProfile.Profile()
        profiler.enable()
        response = Response(img2pdf.convert([im for im in loop_images(id, page_start, page_end)]), mimetype='application/pdf')  
        profiler.disable()      

        # Log the profiling information
        log_buffer = io.StringIO()
        profiler_stats = pstats.Stats(profiler, stream=log_buffer)
        profiler_stats.strip_dirs().sort_stats('cumulative', 'calls').print_stats(20)
        
        formatted_stats = log_buffer.getvalue().splitlines()

        
        current_app.logger.debug(f'==================Profiling information========================: \n')
        for line in formatted_stats:
            current_app.logger.debug(line)
        return response
    except Exception as e:
        return jsonify(Message=str(e)), 400
