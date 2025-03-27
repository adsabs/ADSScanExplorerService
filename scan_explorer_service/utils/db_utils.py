from sqlalchemy import or_
from scan_explorer_service.models import Article, Collection, Page


def collection_exists(session, journal, volume):
    return session.query(Collection).filter(Collection.journal == journal).filter(Collection.volume == volume).first() is not None


def collection_get(session, journal, volume):
    return session.query(Collection).filter(Collection.journal == journal).filter(Collection.volume == volume).one_or_none()


def collection_get_or_create(session, **kwargs):
    collection = Collection(**kwargs)
    persisted = collection_get(session, collection.journal, collection.volume)
    return persisted if persisted else create(session, collection)


def article_exists(session, bibcode):
    return session.query(Article).filter(Article.bibcode == bibcode).first() is not None


def article_get(session, bibcode):
    return session.query(Article).filter(Article.bibcode == bibcode).one_or_none()


def article_get_or_create(session, **kwargs):
    article = Article(**kwargs)
    persisted = article_get(session, article.bibcode)
    return persisted if persisted else create(session, article)


def page_exists(session, collection_id, name, page_num):
    return session.query(Page).filter(Page.collection_id == collection_id).filter(
        or_(Page.name == name, Page.volume_running_page_num == page_num)).first() is not None


def page_get(session, collection_id, name, page_num):
    return session.query(Page).filter(Page.collection_id == collection_id).filter(
        or_(Page.name == name, Page.volume_running_page_num == page_num)).one_or_none()


def page_get_or_create(session, **kwargs):
    page = Page(**kwargs)
    persisted = page_get(session, page.collection_id,
                         page.name, page.volume_running_page_num)
    return persisted if persisted else create(session, page)


def create(session, object):
    session.add(object)
    session.flush()
    session.refresh(object)
    return object


def overwrite(session, object, persisted):
    if persisted:
        session.delete(persisted)
        session.flush()

    create(session, object)


def collection_overwrite(session, collection):
    persisted = collection_get(session, collection.journal, collection.volume)
    overwrite(session, collection, persisted)


def article_overwrite(session, article):
    persisted = article_get(session, article.bibcode)
    overwrite(session, article, persisted)


def page_overwrite(session, page):
    persisted = page_get(session, page.collection_id, page.name, page.volume_running_page_num)
    overwrite(session, page, persisted)

def article_thumbnail(session, id):
    page = session.query(Page).join(Article, Page.articles).filter(
                Article.id == id).order_by(Page.volume_running_page_num.asc()).first()
    return page.thumbnail_url

def collection_thumbnail(session, id):
    page = session.query(Page).filter(Page.collection_id == id).order_by(
        Page.volume_running_page_num.asc()).first()
    return page.thumbnail_url

def page_thumbnail(session, id):
    page = session.query(Page).filter(Page.id == id).one()
    return page.thumbnail_url

def item_thumbnail(session, id, type):
    if type == 'page':
        return page_thumbnail(session, id)
    elif type == 'article':
        return article_thumbnail(session, id)
    elif type == 'collection':
        return collection_thumbnail(session, id)
    else:
        raise Exception("Invalid type")