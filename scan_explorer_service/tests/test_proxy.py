import unittest
from flask_testing import TestCase
from flask import url_for
from unittest.mock import patch
from scan_explorer_service.tests.base import TestCaseDatabase
from scan_explorer_service.views.image_proxy import image_proxy, image_proxy_thumbnail
from scan_explorer_service.models import Article, Base, Collection, Page


class TestProxy(TestCaseDatabase):

    def create_app(self):
        '''Start the wsgi application'''
        from scan_explorer_service.app import create_app
        return create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url,
            'OPEN_SEARCH_URL': 'http://localhost:1234',
            'OPEN_SEARCH_INDEX': 'test',
            'SQLALCHEMY_ECHO': False,
            'TESTING': True,
            'PROPAGATE_EXCEPTIONS': True,
            'TRAP_BAD_REQUEST_ERRORS': True,
            'PRESERVE_CONTEXT_ON_EXCEPTION': False
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)
        self.collection = Collection(
            type='type', journal='journal', volume='volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.collection)

        self.article = Article(bibcode='1988ApJ...333..341R',
                               collection_id=self.collection.id)
        self.app.db.session.add(self.article)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article)

        self.article2 = Article(bibcode='1988ApJ...333..352S',
                                collection_id=self.collection.id)
        self.app.db.session.add(self.article2)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article2)

        self.page = Page(name='page', collection_id=self.collection.id)
        self.page.width = 1000
        self.page.height = 1000
        self.page.label = 'label'
        self.page.volume_running_page_num = 100
        self.app.db.session.add(self.page)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.page)

        self.article.pages.append(self.page)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article)

        self.article2.pages.append(self.page)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article2)

    def mocked_request(*args, **kwargs):
        class Raw:
            def __init__(self, data):
                self.data = data

            def stream(self, decode_content: bool):
                return self.data

        class MockResponse:
            def __init__(self, data, status_code, headers):
                self.raw = Raw(data)
                self.status_code = status_code
                self.headers = headers

            def json(self):
                return self.json_data

        if 'notfound' in args[1]:
            return MockResponse({}, 401, {})
        elif 'badrequest' in args[1]:
            return MockResponse({}, 400, {})
        return MockResponse({}, 200, {})

    @patch('requests.request', side_effect=mocked_request)
    def test_get_image(self, mock_request):

        url = url_for('proxy.image_proxy', path='valid-~image-~path')
        response = self.client.get(url)

        assert(response != None)
        assert(mock_request.called)
        assert(response.status_code == 200)

        url = url_for('proxy.image_proxy', path='notfound-~image-~path')
        response = self.client.get(url)
        assert(response.status_code == 401)

        response = image_proxy('badrequest-~image-~path')
        assert(response.status_code == 400)

    @patch('requests.request', side_effect=mocked_request)
    def test_get_thumbnail(self, mock_request):

        data = {
            'id': '1988ApJ...333..341R',
            'type': 'article'
        }

        url = url_for('proxy.image_proxy_thumbnail', **data)
        response = self.client.get(url)

        assert(response != None)
        assert(mock_request.called)
        assert(response.is_streamed)
        assert(response.status_code == 200)


if __name__ == '__main__':
    unittest.main()
