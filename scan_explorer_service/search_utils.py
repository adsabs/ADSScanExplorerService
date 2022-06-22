import os
import requests
from scan_explorer_service.models import Article, Collection, Page, PageType
from flask_sqlalchemy import Pagination
from flask import current_app
import shlex



collection_query_translations = dict({
    'bibstem': lambda val: Collection.journal.ilike(f'{val}%'),
    'journal': lambda val: Collection.journal.ilike(f'{val}%'),
    'volume': lambda val: Collection.volume.ilike(f'%{val}%'),
})

article_query_translations = dict({
    'bibcode': (lambda val: Article.bibcode.ilike(f'{val}%')),
})

page_query_translations = dict({
    'page': lambda val: Page.label == val,
    'page_collection': lambda val: Page.volume_running_page_num == val,
    'pagetype': lambda val: Page.page_type == PageType.from_string(val)
})


def serialize_result(db_session, result: Pagination, contentQuery = ''):
    return {'page': result.page, 'pageCount': result.pages, 'limit': result.per_page, 'total': result.total, 'query': contentQuery, 
    'items': [item.serialized | fetch_ads_metadata(db_session, item.id) for item in result.items]}



def fetch_ads_metadata(session, uuid: str):
    auth_token = os.getenv('ADS_API_AUTH_TOKEN')
    if auth_token:
        article = session.query(Article).filter_by(id=uuid).one_or_none()
        if article:
            params = {'q': f'bibcode:{article.bibcode}', 'fl':'title,author'}
            headers = {'Authorization': f'Bearer {auth_token}'}
            response = requests.get(current_app.config.get('ADS_API_URL'), params, headers=headers).json()
            docs = response.get('response').get('docs')

            if docs:
                return docs[0]
            
    return {}