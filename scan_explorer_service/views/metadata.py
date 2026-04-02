from datetime import datetime, timezone
from typing import Union
from flask import Blueprint, current_app, jsonify, request
from scan_explorer_service.utils.db_utils import article_get_or_create, article_overwrite, collection_overwrite, page_get_or_create, page_overwrite
from scan_explorer_service.models import Article, Collection, Page, page_article_association_table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from flask_discoverer import advertise
from scan_explorer_service.utils.search_utils import *
from scan_explorer_service.views.view_utils import ApiErrors
from scan_explorer_service.utils.cache import cache_delete_manifest, cache_get_search, cache_set_search
from scan_explorer_service.open_search import EsFields, page_os_search, aggregate_search, page_ocr_os_search
import opensearchpy
import requests
import hashlib
import json as json_lib

bp_metadata = Blueprint('metadata', __name__, url_prefix='/metadata')


@advertise(scopes=['api'], rate_limit=[300, 3600*24])
@bp_metadata.route('/article/extra/<string:bibcode>', methods=['GET'])
def article_extra(bibcode: str):
    """Route that fetches additional metadata about an article from the ADS search service """


    auth_token = current_app.config.get('ADS_SEARCH_SERVICE_TOKEN')
    ads_search_service = current_app.config.get('ADS_SEARCH_SERVICE_URL')

    if auth_token and ads_search_service:
        try:
            params = {'q': f'bibcode:{bibcode}', 'fl':'title,author'}
            headers = {'Authorization': f'Bearer {auth_token}'}
            response = requests.get(ads_search_service, params, headers=headers, timeout=5).json()
            docs = response.get('response').get('docs')
            if docs:
                return docs[0]
            else:
                return jsonify(message='No article found'), 404
        except Exception as e:
            return jsonify(message='Failed to retrieve external ADS article metadata'), 500
    return {}

@advertise(scopes=['api'], rate_limit=[300, 3600*24])
@bp_metadata.route('/article/<string:bibcode>/collection', methods=['GET'])
def article_collection(bibcode: str):
    """Route that fetches collection from an article """
    with current_app.session_scope() as session:
        article: Article = session.query(Article).filter(Article.bibcode == bibcode).first()
        if article is None:
            return jsonify(message='Invalid article bibcode'), 404
        first_page : Page = article.pages.first()
        if first_page is None:
            return jsonify(message='Article has no pages'), 404
        page_in_collection = first_page.volume_running_page_num
        return jsonify({'id': article.collection_id, 'selected_page': page_in_collection}), 200

@advertise(scopes=['ads:scan-explorer'], rate_limit=[300, 3600*24])
@bp_metadata.route('/article', methods=['PUT'])
def put_article():
    """Create a new or overwrite an existing article"""
    json = request.get_json()
    if json:
        with current_app.session_scope() as session:
            try:
                article = Article(**json)
                article_overwrite(session, article)
                return jsonify({'id': article.bibcode}), 200
            except Exception:
                session.rollback()
                return jsonify(message='Failed to create article'), 500
    else:
        return jsonify(message='Invalid article json'), 400


@advertise(scopes=['ads:scan-explorer'], rate_limit=[300, 3600*24])
@bp_metadata.route('/collection', methods=['PUT'])
def put_collection():
    """ Create a new or overwrite an existing collection """
    json = request.get_json()
    if json:
        with current_app.session_scope() as session:
            try:
                collection = Collection(**json)
                collection_overwrite(session, collection)

                now = datetime.now(timezone.utc).replace(tzinfo=None)
                pages_data = []
                articles_data = {}
                page_article_data = []

                for page_json in json.get('pages', []):
                    page_json['collection_id'] = collection.id
                    articles = page_json.pop('articles', [])
                    page = Page(**page_json)
                    pages_data.append({
                        'id': page.id,
                        'name': page.name,
                        'label': page.label,
                        'format': page.format,
                        'color_type': page.color_type,
                        'page_type': page.page_type,
                        'width': page.width,
                        'height': page.height,
                        'collection_id': page.collection_id,
                        'volume_running_page_num': page.volume_running_page_num,
                        'created': now,
                        'updated': now,
                    })
                    for article_json in articles:
                        bibcode = article_json['bibcode']
                        if bibcode not in articles_data:
                            articles_data[bibcode] = {
                                'id': bibcode,
                                'bibcode': bibcode,
                                'collection_id': collection.id,
                                'created': now,
                                'updated': now,
                            }
                        page_article_data.append({
                            'page_id': page.id,
                            'article_id': bibcode,
                        })

                if pages_data:
                    session.bulk_insert_mappings(Page, pages_data)
                if articles_data:
                    session.execute(
                        pg_insert(Article.__table__).values(list(articles_data.values())).on_conflict_do_nothing()
                    )
                if page_article_data:
                    session.execute(
                        pg_insert(page_article_association_table).values(page_article_data).on_conflict_do_nothing()
                    )
                session.commit()
                cache_delete_manifest(collection.id)

                return jsonify({'id': collection.id}), 200
            except Exception:
                session.rollback()
                return jsonify(message='Failed to create collection'), 500
    else:
        return jsonify(message='Invalid collection json'), 400


@advertise(scopes=['ads:scan-explorer'], rate_limit=[300, 3600*24])
@bp_metadata.route('/page', methods=['PUT'])
def put_page():
    """Create a new or overwrite an existing page """
    json = request.get_json()
    if json:
        with current_app.session_scope() as session:
            try:
                page = Page(**json)
                page_overwrite(session, page)

                for article_json in json.get('articles', []):
                    article_json['collection_id'] = page.collection_id
                    page.articles.append(article_get_or_create(session, **article_json))

                session.add(page)
                session.commit()
                session.refresh(page)
                return jsonify({'id': page.id}), 200
            except Exception:
                session.rollback()
                return jsonify(message='Failed to create page'), 500
    else:
        return jsonify(message='Invalid page json'), 400


def _make_search_cache_key(prefix, args):
    """Build an MD5 cache key from the search type prefix and all query params (including multi-valued)."""
    raw = prefix + str(sorted(args.items(multi=True)))
    return hashlib.md5(raw.encode()).hexdigest()


@advertise(scopes=['api'], rate_limit=[300, 3600*24])
@bp_metadata.route('/article/search', methods=['GET'])
def article_search():
    """Search for an article using one or some of the available keywords"""
    try:
        qs, qs_dict, page, limit, sort = parse_query_args(request.args)

        cache_key = _make_search_cache_key('article', request.args)
        cached = cache_get_search(cache_key)
        if cached is not None:
            return current_app.response_class(cached, content_type='application/json')
        result = aggregate_search(qs, EsFields.article_id, page, limit, sort)
        text_query = ''
        if SearchOptions.FullText.value in qs_dict.keys():
            text_query = qs_dict[SearchOptions.FullText.value]

        article_count = result['aggregations']['total_count']['value']
        collection_count = page_count = 0
        if article_count == 0:
            collection_count = aggregate_search(qs, EsFields.volume_id, page, limit, sort)['aggregations']['total_count']['value']
            page_count = page_os_search(qs, page, limit, sort)['hits']['total']['value']
        agg_limit = current_app.config.get("OPEN_SEARCH_AGG_BUCKET_LIMIT", 10000)
        response_data = serialize_os_article_result(result, page, limit, text_query, collection_count, page_count, agg_limit)
        cache_set_search(cache_key, json_lib.dumps(response_data))
        return jsonify(response_data)
    except (opensearchpy.exceptions.ConnectionError, opensearchpy.exceptions.ConnectionTimeout, opensearchpy.exceptions.TransportError) as e:
        current_app.logger.exception(f"OpenSearch error: {e}")
        return jsonify(message='Search service temporarily unavailable', type=ApiErrors.SearchError.value), 503
    except Exception as e:
        current_app.logger.exception(f"An exception has occurred: {e}")
        return jsonify(message=str(e), type=ApiErrors.SearchError.value), 400


@advertise(scopes=['api'], rate_limit=[300, 3600*24])
@bp_metadata.route('/collection/search', methods=['GET'])
def collection_search():
    """Search for a collection using one or some of the available keywords"""
    try:
        qs, qs_dict, page, limit, sort = parse_query_args(request.args)

        cache_key = _make_search_cache_key('collection', request.args)
        cached = cache_get_search(cache_key)
        if cached is not None:
            return current_app.response_class(cached, content_type='application/json')
        result = aggregate_search(qs, EsFields.volume_id, page, limit, sort)
        text_query = ''
        if SearchOptions.FullText.value in qs_dict.keys():
            text_query = qs_dict[SearchOptions.FullText.value]
        agg_limit = current_app.config.get("OPEN_SEARCH_AGG_BUCKET_LIMIT", 10000)
        response_data = serialize_os_collection_result(result, page, limit, text_query, agg_limit)
        cache_set_search(cache_key, json_lib.dumps(response_data))
        return jsonify(response_data)
    except (opensearchpy.exceptions.ConnectionError, opensearchpy.exceptions.ConnectionTimeout, opensearchpy.exceptions.TransportError) as e:
        current_app.logger.exception(f"OpenSearch error: {e}")
        return jsonify(message='Search service temporarily unavailable', type=ApiErrors.SearchError.value), 503
    except Exception as e:
        return jsonify(message=str(e), type=ApiErrors.SearchError.value), 400

@advertise(scopes=['api'], rate_limit=[300, 3600*24])
@bp_metadata.route('/page/search', methods=['GET'])
def page_search():
    """Search for a page using one or some of the available keywords"""
    try:
        qs, qs_dict, page, limit, sort = parse_query_args(request.args)

        cache_key = _make_search_cache_key('page', request.args)
        cached = cache_get_search(cache_key)
        if cached is not None:
            return current_app.response_class(cached, content_type='application/json')
        result = page_os_search(qs, page, limit, sort)
        text_query = ''
        if SearchOptions.FullText.value in qs_dict.keys():
            text_query = qs_dict[SearchOptions.FullText.value]
        response_data = serialize_os_page_result(result, page, limit, text_query)
        cache_set_search(cache_key, json_lib.dumps(response_data))
        return jsonify(response_data)
    except (opensearchpy.exceptions.ConnectionError, opensearchpy.exceptions.ConnectionTimeout, opensearchpy.exceptions.TransportError) as e:
        current_app.logger.exception(f"OpenSearch error: {e}")
        return jsonify(message='Search service temporarily unavailable', type=ApiErrors.SearchError.value), 503
    except Exception as e:
        return jsonify(message=str(e), type=ApiErrors.SearchError.value), 400

@advertise(scopes=['api'], rate_limit=[300, 3600*24])
@bp_metadata.route('/page/ocr', methods=['GET'])
def get_page_ocr():
    """Get the OCR for a page using it's parents id and page number"""
    try:
        id = request.args.get('id')
        page_number = request.args.get('page_number', 1, int)

        cache_key = _make_search_cache_key('ocr', request.args)
        cached = cache_get_search(cache_key)
        if cached is not None:
            return current_app.response_class(cached, content_type='text/plain')

        with current_app.session_scope() as session:
            item: Union[Article, Collection] = (
                    session.query(Article).filter(Article.id == id).one_or_none()
                    or session.query(Collection).filter(Collection.id == id).one_or_none())

            if item is None:
                return jsonify(message=f'Item with ID {id} was not found'), 404
            elif isinstance(item, Article):
                collection_id = item.collection_id
                first_page = item.pages.first()
                if first_page is None:
                    return jsonify(message=f'Article {id} has no pages'), 404
                page_number = page_number + first_page.volume_running_page_num - 1
            elif isinstance(item, Collection):
                collection_id = item.id

            result = page_ocr_os_search(collection_id, page_number)
            ocr_text = serialize_os_page_ocr_result(result)
            cache_set_search(cache_key, ocr_text)
            return current_app.response_class(ocr_text, content_type='text/plain')

    except (opensearchpy.exceptions.ConnectionError, opensearchpy.exceptions.ConnectionTimeout, opensearchpy.exceptions.TransportError) as e:
        current_app.logger.exception(f"OpenSearch error: {e}")
        return jsonify(message='Search service temporarily unavailable', type=ApiErrors.SearchError.value), 503
    except Exception as e:
        return jsonify(message=str(e), type=ApiErrors.SearchError.value), 400
