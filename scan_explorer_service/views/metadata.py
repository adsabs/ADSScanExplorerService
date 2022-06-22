from flask import Blueprint, current_app, jsonify, request
from scan_explorer_service.db_utils import article_get_or_create, article_overwrite, collection_overwrite, page_get_or_create, page_overwrite
from scan_explorer_service.models import Article, Collection, Page
from flask_discoverer import advertise
from scan_explorer_service.schema.metadata_schema import PaginatedArticlesSchema, PaginatedCollectionsSchema, PaginatedPagesSchema, SearchQuerySchema
from scan_explorer_service.search_utils import *
from flask_sqlalchemy import Pagination
from scan_explorer_service.open_search import EsFields, text_search_aggregate_ids
from sqlalchemy.orm import load_only
from sqlalchemy import func

bp_metadata = Blueprint('metadata', __name__, url_prefix='/metadata')


@advertise(scopes=['put_article'], rate_limit=[300, 3600*24])
@bp_metadata.route('/article', methods=['PUT'])
def put_article():
    """Endpoint used to create a new or overwrite an existing article.
    ---
    parameters:
      - in: body
        name: article
        description: The article to create.
        schema:
            $ref: '#/definitions/Article'
    responses:
      200:
        description: The ID of the newly created resource.
        schema:
            properties:
                id:
                    type: string
                    description: The ID of the newly created resource.
    """
    json = request.get_json()
    if json:
        with current_app.session_scope() as session:
            try:
                article = Article(**json)
                article_overwrite(session, article)
                return jsonify({'id': article.bibcode}), 200
            except:
                session.rollback()
                return jsonify(message='Failed to create article'), 500
    else:
        return jsonify(message='Invalid article json'), 400


@advertise(scopes=['put_collection'], rate_limit=[300, 3600*24])
@bp_metadata.route('/collection', methods=['PUT'])
def put_collection():
    """Endpoint used to create a new or overwrite an existing collection.
    ---
    parameters:
      - in: body
        name: collection
        description: The collection to create.
        schema:
            $ref: '#/definitions/Collection'
    responses:
      200:
        description: The ID of the newly created resource.
        schema:
            properties:
                id:
                    type: string
                    description: The ID of the newly created resource.
    """
    json = request.get_json()
    if json:
        with current_app.session_scope() as session:
            try:
                collection = Collection(**json)
                collection_overwrite(session, collection)

                for page_json in json.get('pages', []):
                    page_json['collection_id'] = collection.id
                    page = page_get_or_create(session, **page_json)

                    for article_json in page_json.get('articles', []):
                        article_json['collection_id'] = collection.id
                        page.articles.append(article_get_or_create(session, **article_json))

                    session.add(page)
                session.commit()

                return jsonify({'id': collection.id}), 200
            except:
                session.rollback()
                return jsonify(message='Failed to create collection'), 500
    else:
        return jsonify(message='Invalid collection json'), 400


@advertise(scopes=['put_page'], rate_limit=[300, 3600*24])
@bp_metadata.route('/page', methods=['PUT'])
def put_page():
    """Endpoint used to create a new or overwrite an existing page.
    ---
    parameters:
      - in: body
        name: page
        description: The page to create.
        schema:
            $ref: '#/definitions/Page'
    responses:
      200:
        description: The ID of the newly created resource.
        schema:
            properties:
                id:
                    type: string
                    description: The ID of the newly created resource.
    """
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
            except:
                session.rollback()
                return jsonify(message='Failed to create page'), 500
    else:
        return jsonify(message='Invalid page json'), 400


@advertise(scopes=['article_search'], rate_limit=[300, 3600*24])
@bp_metadata.route('/article/search', methods=['GET'])
def article_search():
    """Endpoint used to perform text search in an article.
    ---
    parameters:
      - name: q
        in: query
        type: string
        required: true
        description: Space seperated search query using predefined keys.
      - name: page
        in: query
        type: number
        required: false
        description: Page offset, between 1 - n.
      - name: limit
        in: query
        type: number
        required: false
        description: Number of results to display on each page.
    responses:
      200:
        description: A paginated list of pages matching the search criteria.
        schema:
            $ref: '#/definitions/PaginatedArticles'
    """
    search_query = SearchQuerySchema().load(request.args)
    qs_dict = search_query.get('q')
    page = search_query.get('page', 1)
    limit = search_query.get('limit', 10)

    query_trans = {key: filter_func for key, filter_func in article_query_translations.items() if key in qs_dict.keys()}
    jv_query_trans = {key: filter_func for key, filter_func in collection_query_translations.items() if key in qs_dict.keys()}

    with current_app.session_scope() as session:
        query = session.query(Article).join(Page, Article.pages)
        for key, filter_func in query_trans.items():
            query = query.filter(filter_func(qs_dict.get(key)))

        if len(jv_query_trans) > 0:
            query = query.join(Collection)
            for key, filter_func in jv_query_trans.items():
                query = query.filter(filter_func(qs_dict.get(key)))

        if 'full' in qs_dict.keys():
            item_ids = [str(a.bibcode) for a in query.options(load_only('bibcode')).all()]
            es_ids, es_counts = text_search_aggregate_ids(
                qs_dict.get('full'), EsFields.article_id, EsFields.article_id, item_ids)
            if len(es_ids) > 0:
                query = session.query(Article).filter(Article.bibcode.in_(es_ids))
                subq = session.query(
                    func.unnest(es_ids).label('id'),
                    func.unnest(es_counts).label('count')
                ).subquery()
                query = query.join(subq, Article.bibcode == subq.c.id).order_by(subq.c.count.desc())
        else:
            query = query.group_by(Article.id).order_by(Article.collection_id, func.min(Page.volume_running_page_num))

        result: Pagination = query.paginate(page, limit, False)

        return jsonify(PaginatedArticlesSchema().dump(result))


@advertise(scopes=['collection_search'], rate_limit=[300, 3600*24])
@bp_metadata.route('/collection/search', methods=['GET'])
def collection_search():
    """Endpoint used to perform text search in a collection.
    ---
    parameters:
      - name: q
        in: query
        type: string
        required: true
        description: Space seperated search query using predefined keys.
      - name: page
        in: query
        type: number
        required: false
        description: Page offset, between 1 - n.
      - name: limit
        in: query
        type: number
        required: false
        description: Number of results to display on each page.
    responses:
      200:
        description: A paginated list of collections matching the search criteria.
        schema:
            $ref: '#/definitions/PaginatedCollections'
    """
    search_query = SearchQuerySchema().load(request.args)
    qs_dict = search_query.get('q')
    page = search_query.get('page', 1)
    limit = search_query.get('limit', 10)

    query_trans = {key: filter_func for key,filter_func in collection_query_translations.items() if key in qs_dict.keys()}
    a_query_trans = {key: filter_func for key,filter_func in article_query_translations.items() if key in qs_dict.keys()}

    with current_app.session_scope() as session:
        query = session.query(Collection)
        for key, filter_func in query_trans.items():
            query = query.filter(filter_func(qs_dict.get(key)))

        if len(a_query_trans) > 0:
            query = query.join(Article)
            for key, filter_func in a_query_trans.items():
                query = query.filter(filter_func(qs_dict.get(key)))

        if 'full' in qs_dict.keys():
            item_ids = [str(a.id) for a in query.options(load_only('id')).all()]
            es_ids, es_counts = text_search_aggregate_ids(
                qs_dict.get('full'), EsFields.volume_id, EsFields.volume_id, item_ids)
            if len(es_ids) > 0:
                query = session.query(Collection).filter(Collection.id.in_(es_ids))
                subq = session.query(
                    func.unnest(es_ids).label('id'),
                    func.unnest(es_counts).label('count')
                ).subquery()
                query = query.join(subq, Collection.id == subq.c.id).order_by(subq.c.count.desc())
        else:
            query = query.group_by(Collection.id).order_by(Collection.id)

        result: Pagination = query.paginate(page, limit, False)

        return jsonify(PaginatedCollectionsSchema().dump(result))


@advertise(scopes=['page_search'], rate_limit=[300, 3600*24])
@bp_metadata.route('/page/search', methods=['GET'])
def page_search():
    """Endpoint used to perform text search in a page.
    ---
    parameters:
      - name: q
        in: query
        type: string
        required: true
        description: Space seperated search query using predefined keys.
      - name: page
        in: query
        type: number
        required: false
        description: Page offset, between 1 - n.
      - name: limit
        in: query
        type: number
        required: false
        description: Number of results to display on each page.
    responses:
      200:
        description: A paginated list of pages matching the search criteria.
        schema:
            $ref: '#/definitions/PaginatedPages'
    """
    search_query = SearchQuerySchema().load(request.args)
    qs_dict = search_query.get('q')
    page = search_query.get('page', 1)
    limit = search_query.get('limit', 10)

    query_trans = {key: filter_func for key, filter_func in page_query_translations.items() if key in qs_dict.keys()}
    a_query_trans = {key: filter_func for key, filter_func in article_query_translations.items() if key in qs_dict.keys()}
    jw_query_trans = {key: filter_func for key, filter_func in collection_query_translations.items() if key in qs_dict.keys()}

    with current_app.session_scope() as session:
        if 'full' in qs_dict.keys():
            query = page_search_text_search(qs_dict)
        else:
            query = session.query(Page)
            for key, filter_func in query_trans.items():
                query = query.filter(filter_func(qs_dict.get(key)))

            if len(a_query_trans) > 0:
                query = query.join(Article, Page.articles)
                for key, filter_func in a_query_trans.items():
                    query = query.filter(filter_func(qs_dict.get(key)))

            if len(jw_query_trans) > 0:
                query = query.join(Collection)
                for key, filter_func in jw_query_trans.items():
                    query = query.filter(filter_func(qs_dict.get(key)))
            query = query.group_by(Page.id)
            query.order_by(Collection.id, Page.volume_running_page_num)

        result: Pagination = query.paginate(page, limit, False)

        return jsonify(PaginatedPagesSchema().dump(result))


def page_search_text_search(qs_dict):
    query_trans = {key: filter_func for key, filter_func in page_query_translations.items() if key in qs_dict.keys()}
    a_query_trans = {key: filter_func for key, filter_func in article_query_translations.items() if key in qs_dict.keys()}
    jw_query_trans = {key: filter_func for key, filter_func in collection_query_translations.items() if key in qs_dict.keys()}

    with current_app.session_scope() as session:
        filter_field = None
        if len(query_trans) > 0:
            filter_field = EsFields.page_id
            query = session.query(Page)
            for key, filter_func in query_trans.items():
                query = query.filter(filter_func(qs_dict.get(key)))

        if len(a_query_trans) > 0:
            if filter_field == EsFields.page_id:
                query = query.join(Article, Page.articles)
            else:
                filter_field = EsFields.article_id
                query = session.query(Article)
            for key, filter_func in a_query_trans.items():
                query = query.filter(filter_func(qs_dict.get(key)))

        if len(jw_query_trans) > 0:
            if filter_field == EsFields.page_id or filter_field == EsFields.article_id:
                query = query.join(Collection, Page.collection)
            else:
                filter_field = EsFields.volume_id
                query = session.query(Collection)

            for key, filter_func in jw_query_trans.items():
                query = query.filter(filter_func(qs_dict.get(key)))

        if filter_field:
            item_ids = [str(a.id) for a in query.options(load_only('id')).all()]
        else:
            item_ids = None
        es_ids, _ = text_search_aggregate_ids(
            qs_dict.get('full'), filter_field, EsFields.page_id, item_ids)
        if len(es_ids) > 0:
            query = session.query(Page).filter(Page.id.in_(es_ids))
            query = query.order_by(
                Page.collection_id, Page.volume_running_page_num)
        else:
            query = session.query(Page).filter(False)
        return query