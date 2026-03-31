
from flask import Blueprint, current_app, jsonify, request, Response
from flask_restful import abort
from scan_explorer_service.extensions import manifest_factory
from scan_explorer_service.models import Article, Page, Collection
from flask_discoverer import advertise
from scan_explorer_service.open_search import EsFields, text_search_highlight
from scan_explorer_service.utils.utils import proxy_url, url_for_proxy
from sqlalchemy.orm import selectinload
from typing import Union
import json as json_lib
import hashlib
import redis
import logging
import threading

logger = logging.getLogger(__name__)

MANIFEST_CACHE_TTL = 3600
MANIFEST_CACHE_PREFIX = 'scan:manifest:'
SEARCH_CACHE_TTL = 60
SEARCH_CACHE_PREFIX = 'scan:search:'

_redis_client = None
_redis_lock = threading.Lock()


def _get_redis():
    """Return the singleton Redis client, creating it on first call with double-checked locking."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    with _redis_lock:
        # Double-checked locking: another thread may have connected while we waited for the lock
        if _redis_client is not None:
            return _redis_client
        try:
            url = current_app.config.get('REDIS_URL', 'redis://redis-backend:6379/4')
            client = redis.from_url(url, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
            client.ping()
            _redis_client = client
            return _redis_client
        except Exception:
            logger.warning("Redis unavailable, manifest caching disabled")
            return None


def _reset_redis():
    """Clear the cached Redis client so the next call to _get_redis reconnects."""
    global _redis_client
    _redis_client = None


def _cache_get(key):
    """Fetch a cached manifest JSON string by key, returning None on miss or Redis failure."""
    r = _get_redis()
    if r is None:
        return None
    try:
        return r.get(MANIFEST_CACHE_PREFIX + key)
    except redis.ConnectionError:
        _reset_redis()
        return None
    except Exception:
        return None


def _cache_set(key, json_str):
    """Store a manifest JSON string in Redis with TTL. Silently resets on connection failure."""
    r = _get_redis()
    if r is None:
        return
    try:
        r.setex(MANIFEST_CACHE_PREFIX + key, MANIFEST_CACHE_TTL, json_str)
    except redis.ConnectionError:
        _reset_redis()
    except Exception:
        logger.debug("Failed to write manifest cache for key %s", key, exc_info=True)


def _cache_delete(key):
    """Remove a manifest cache entry. Called when a collection is overwritten via PUT."""
    r = _get_redis()
    if r is None:
        return
    try:
        r.delete(MANIFEST_CACHE_PREFIX + key)
    except redis.ConnectionError:
        _reset_redis()
    except Exception:
        logger.debug("Failed to delete manifest cache for key %s", key, exc_info=True)


def _search_cache_get(key):
    """Fetch a cached search result by key. Uses a shorter TTL than manifests."""
    r = _get_redis()
    if r is None:
        return None
    try:
        return r.get(SEARCH_CACHE_PREFIX + key)
    except redis.ConnectionError:
        _reset_redis()
        return None
    except Exception:
        return None


def _search_cache_set(key, json_str):
    """Store a search result in Redis with a short TTL (60s)."""
    r = _get_redis()
    if r is None:
        return
    try:
        r.setex(SEARCH_CACHE_PREFIX + key, SEARCH_CACHE_TTL, json_str)
    except redis.ConnectionError:
        _reset_redis()
    except Exception:
        logger.debug("Failed to write search cache for key %s", key, exc_info=True)


bp_manifest = Blueprint('manifest', __name__, url_prefix='/manifest')


@bp_manifest.before_request
def before_request():
    """Configure manifest_factory base URIs from the proxy URL before each request."""
    server, prefix = proxy_url()
    base_uri = f'{server}/{prefix}/manifest'
    manifest_factory.set_base_prezi_uri(base_uri)

    image_proxy = url_for_proxy('proxy.image_proxy', path='')
    manifest_factory.set_base_image_uri(image_proxy)


@advertise(scopes=['api'], rate_limit=[300, 3600*24])
@bp_manifest.route('/<string:id>/manifest.json', methods=['GET'])
def get_manifest(id: str):
    """ Creates an IIIF manifest from an article or Collection"""

    cached = _cache_get(id)
    if cached is not None:
        return Response(cached, content_type='application/json')

    with current_app.session_scope() as session:
        item = session.query(Article).filter(Article.id == id).one_or_none()

        if item:
            manifest = manifest_factory.create_manifest(item)
            search_url = url_for_proxy('manifest.search', id=id)
            manifest_factory.add_search_service(manifest, search_url)
            result = manifest.toJSON(top=True)
            result_json = json_lib.dumps(result) if isinstance(result, dict) else result
            _cache_set(id, result_json)
            return result

        collection = session.query(Collection).filter(Collection.id == id).one_or_none()

        if collection:
            pages = session.query(Page)\
                .filter(Page.collection_id == id)\
                .options(selectinload(Page.articles))\
                .order_by(Page.volume_running_page_num)\
                .all()

            articles = session.query(Article)\
                .filter(Article.collection_id == id)\
                .all()

            article_pages = {}
            for page in pages:
                for article in page.articles:
                    article_pages.setdefault(article.id, []).append(page)

            manifest = manifest_factory.create_collection_manifest(
                collection, pages, articles, article_pages)
            search_url = url_for_proxy('manifest.search', id=id)
            manifest_factory.add_search_service(manifest, search_url)
            result = manifest.toJSON(top=True)
            result_json = json_lib.dumps(result) if isinstance(result, dict) else result
            _cache_set(id, result_json)
            return result

        return jsonify(exception='Article not found'), 404


@advertise(scopes=['api'], rate_limit=[300, 3600*24])
@bp_manifest.route('/canvas/<string:page_id>.json', methods=['GET'])
def get_canvas(page_id: str):
    """ Creates an IIIF canvas from a page"""
    with current_app.session_scope() as session:
        page = session.query(Page).filter(Page.id == page_id).first()
        if page:
            canvas = manifest_factory.get_or_create_canvas(page)
            return canvas.toJSON(top=True)
        else:
            return jsonify(exception='Page not found'), 404


@advertise(scopes=['api'], rate_limit=[300, 3600*24])
@bp_manifest.route('/<string:id>/search', methods=['GET'])
def search(id: str):
    """ Searches the content of an article """

    query = request.args.get('q')
    if not query or len(query) <= 0:
        return jsonify(exception='No search query specified'), 400

    cache_key = hashlib.md5(f"{id}:{query}".encode()).hexdigest()
    cached = _search_cache_get(cache_key)
    if cached is not None:
        return Response(cached, content_type='application/json')

    with current_app.session_scope() as session:
        item: Union[Article, Collection] = (
                    session.query(Article).filter(Article.id == id).one_or_none()
                    or session.query(Collection).filter(Collection.id == id).one_or_none())
        if item:
            annotation_list = manifest_factory.annotationList(request.url)
            annotation_list.resources = []

            es_field = EsFields.article_id if isinstance(item, Article) else EsFields.volume_id
            results = text_search_highlight(query, es_field, item.id)

            for res in results:
                annotation = annotation_list.annotation(res['page_id'])
                canvas_slice_url = url_for_proxy('manifest.get_canvas', page_id=res['page_id'])
                annotation.on = canvas_slice_url
                highlight_text = "<br><br>".join(res['highlight']).replace("em>", "b>")
                annotation.text(highlight_text, format="text/html")

            result = annotation_list.toJSON(top=True)
            result_json = json_lib.dumps(result) if isinstance(result, dict) else result
            _search_cache_set(cache_key, result_json)
            return result

        else:
            return jsonify(exception='Article or volume not found'), 404
