import unittest
import json
from flask import url_for
from unittest.mock import MagicMock, patch
from scan_explorer_service.tests.base import TestCaseDatabase
from scan_explorer_service.views.image_proxy import image_proxy, get_item
from scan_explorer_service.models import Article, Base, Collection, Page
from scan_explorer_service.views.image_proxy import img2pdf, fetch_images, fetch_object

class TestProxy(TestCaseDatabase):
    """Tests for image proxy, thumbnail, PDF, and S3 fetch endpoints."""

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

        self.page1 = Page(name='page1', collection_id=self.collection.id)
        self.page1.width = 1000
        self.page1.height = 1000
        self.page1.label = 'label'
        self.page1.volume_running_page_num = 101
        self.app.db.session.add(self.page1)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.page1)
        
        self.article.pages.append(self.page)
        self.article.pages.append(self.page1)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article)

        self.article2.pages.append(self.page)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article2)

    def mocked_request(*args, **kwargs):
        """Return mock HTTP responses based on URL path keywords."""
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

            def close(self):
                pass

        if 'notfound' in args[1]:
            return MockResponse({}, 401, {})
        elif 'badrequest' in args[1]:
            return MockResponse({}, 400, {})
        return MockResponse({}, 200, {})

    @patch('requests.request', side_effect=mocked_request)
    def test_get_image(self, mock_request):
        """Verifies that image proxy forwards requests and returns correct status codes."""
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

    @patch('scan_explorer_service.views.image_proxy.requests.request')
    def test_image_proxy_closes_upstream_response(self, mock_request):
        """Verifies that the upstream response is closed after the streamed response completes."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.raw.stream.return_value = [b'chunk1', b'chunk2']
        mock_request.return_value = mock_response

        url = url_for('proxy.image_proxy', path='some-~image-~path')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        response.close()
        mock_response.close.assert_called()

    @patch('requests.request', side_effect=mocked_request)
    def test_get_thumbnail(self, mock_request):
        """Verifies that thumbnail proxy returns a streamed 200 response for a valid article."""
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

    def test_get_item(self):
        """Test retrieving an item by its ID"""
        with self.app.app_context():
            article = get_item(self.app.db.session, self.article.id)
            assert(isinstance(article, Article))

            collection = get_item(self.app.db.session, self.collection.id)
            assert(isinstance(collection, Collection))

            with self.assertRaises(Exception) as context:
                get_item(self.app.db.session, 'non-existent-id')
            assert("ID: non-existent-id not found" in str(context.exception))

    @patch('scan_explorer_service.views.image_proxy.S3Provider')
    def test_fetch_images(self, mock_s3_cls):
        """Verifies that fetch_images yields image bytes for each page in the range."""
        mock_s3 = MagicMock()
        mock_s3.read_object_s3.return_value = b'image_data'
        mock_s3_cls.return_value = mock_s3
        item = self.article
        page_start = 1
        page_end = 2
        page_limit = 5
        memory_limit = 100

        gen = fetch_images(self.app.db.session, item, page_start, page_end, page_limit, memory_limit)
        images = list(gen)
        self.assertEqual(images, [b'image_data', b'image_data'])
        mock_s3.read_object_s3.assert_called()

    @patch('scan_explorer_service.utils.s3_utils.S3Provider.read_object_s3')
    def test_fetch_object(self, mock_read_object_s3):
        """Verifies that fetch_object reads the correct S3 object and returns its bytes."""
        object_name = 'bitmaps/type/journal/volume/600/page'
        mock_read_object_s3.return_value = b'image-data'

        self.app.config['AWS_BUCKET_NAME'] = 'bucket-name'
        
        result = fetch_object(object_name, 'AWS_BUCKET_NAME')
        
        mock_read_object_s3.assert_called_once_with(object_name)
        self.assertEqual(result, b'image-data')

    @patch('scan_explorer_service.views.image_proxy.fetch_object')
    def test_pdf_save_success_article(self, mock_fetch_object):
        """Verifies that PDF download for an article returns 200 with application/pdf content type."""
        mock_fetch_object.return_value = b'my_image_name'

        data = {
            'id': self.article.id,  
        }
        
        response = self.client.get(url_for('proxy.pdf_save', **data))
        
        assert(response.status_code == 200)
        assert('application/pdf' == response.content_type)
        assert(b'my_image_name' in response.data)
        mock_fetch_object.assert_called()

    @patch('scan_explorer_service.views.image_proxy.img2pdf.convert')
    @patch('scan_explorer_service.views.image_proxy.fetch_images')
    def test_pdf_save_success_collection(self, mock_fetch_images, mock_img2pdf_convert):
        """Verifies that PDF download for a collection page range returns converted PDF data."""
        mock_fetch_images.return_value = [b'image_data_1', b'image_data_2', b'image_data_3']

        mock_img2pdf_convert.return_value = b'pdf_data'

        data = {
            'id': self.collection.id,  
            'page_start': 1, 
            'page_end': 3
        }
        
        response = self.client.get(url_for('proxy.pdf_save', **data))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, 'application/pdf')
        self.assertEqual(response.data, b'pdf_data')

        mock_img2pdf_convert.assert_called_once_with([b'image_data_1', b'image_data_2', b'image_data_3'])


class TestImageProxyRetry(TestCaseDatabase):
    """Test Cantaloupe cold-cache retry logic in image_proxy."""

    def create_app(self):
        from scan_explorer_service.app import create_app
        return create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'TESTING': True,
            'PROPAGATE_EXCEPTIONS': True,
            'TRAP_BAD_REQUEST_ERRORS': True,
            'PRESERVE_CONTEXT_ON_EXCEPTION': False,
            'IMAGE_PROXY_RETRIES': 1,
            'IMAGE_PROXY_RETRY_DELAY': 0,
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)

    def _make_mock_response(self, data, status_code, headers=None):
        """Build a mock HTTP response with streamable raw data."""
        class Raw:
            def __init__(self, d):
                self.data = d
            def stream(self, decode_content=False):
                return self.data
        class MockResponse:
            def __init__(self, d, sc, h):
                self.raw = Raw(d)
                self.status_code = sc
                self.headers = h or {}
            def close(self):
                pass
        return MockResponse(data, status_code, headers or {})

    @patch('requests.request')
    def test_retry_on_cold_cache_400(self, mock_request):
        """Verifies that a 400 from Cantaloupe triggers a retry that succeeds."""
        fail = self._make_mock_response([b'error'], 400)
        success = self._make_mock_response([b'ok'], 200)
        mock_request.side_effect = [fail, success]

        url = url_for('proxy.image_proxy', path='some-~image-~path')
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_request.call_count, 2)

    @patch('requests.request')
    def test_retry_on_cold_cache_500(self, mock_request):
        """Verifies that a 500 from Cantaloupe triggers a retry that succeeds."""
        fail = self._make_mock_response([b'error'], 500)
        success = self._make_mock_response([b'ok'], 200)
        mock_request.side_effect = [fail, success]

        url = url_for('proxy.image_proxy', path='some-~image-~path')
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_request.call_count, 2)

    @patch('requests.request')
    def test_no_retry_on_success(self, mock_request):
        """Verifies that a successful response does not trigger any retries."""
        success = self._make_mock_response([b'ok'], 200)
        mock_request.return_value = success

        url = url_for('proxy.image_proxy', path='some-~image-~path')
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_request.call_count, 1)

    @patch('requests.request')
    def test_returns_error_after_exhausted_retries(self, mock_request):
        """Verifies that the error response is returned after all retries are exhausted."""
        fail = self._make_mock_response([b'error'], 400)
        mock_request.return_value = fail

        url = url_for('proxy.image_proxy', path='some-~image-~path')
        response = self.client.get(url)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(mock_request.call_count, 2)


class TestProxyNullHandling(TestCaseDatabase):
    """Tests for S4 and S6: null/error handling in image_proxy.py."""

    def create_app(self):
        from scan_explorer_service.app import create_app
        return create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url,
            'OPEN_SEARCH_URL': 'http://localhost:1234',
            'OPEN_SEARCH_INDEX': 'test',
            'SQLALCHEMY_ECHO': False,
            'TESTING': True,
            'PROPAGATE_EXCEPTIONS': True,
            'TRAP_BAD_REQUEST_ERRORS': True,
            'PRESERVE_CONTEXT_ON_EXCEPTION': False,
            'IMAGE_PDF_MEMORY_LIMIT': 100 * 1024 * 1024,
            'IMAGE_PDF_PAGE_LIMIT': 100,
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)

        self.collection = Collection(type='type', journal='journal', volume='volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.collection)

        self.article_no_pages = Article(bibcode='2000ApJ...001..099Z',
                                         collection_id=self.collection.id)
        self.app.db.session.add(self.article_no_pages)
        self.app.db.session.commit()
        self.article_no_pages_id = self.article_no_pages.id

    @patch('scan_explorer_service.views.image_proxy.fetch_object')
    def test_pdf_save_article_no_pages_returns_400(self, mock_fetch_object):
        """Verifies that PDF download returns 400 when article has no pages and no pre-built PDF."""
        mock_fetch_object.side_effect = ValueError("File content is empty")

        response = self.client.get(url_for('proxy.pdf_save', id=self.article_no_pages_id))
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertIn('No pages found', data['Message'])

    def test_get_pages_article_no_pages_raises(self):
        """Verifies that get_pages raises an exception for an article with no pages."""
        from scan_explorer_service.views.image_proxy import get_pages
        with self.app.app_context():
            with self.assertRaises(Exception) as ctx:
                get_pages(self.article_no_pages, self.app.db.session, 1, 10, 100)
            self.assertIn('No pages found', str(ctx.exception))

    @patch('scan_explorer_service.views.image_proxy.fetch_object')
    def test_fetch_article_exception_no_unbound_local(self, mock_fetch_object):
        """Verifies that fetch_article handles S3 exceptions without an UnboundLocalError."""
        mock_fetch_object.side_effect = ValueError("S3 error")
        from scan_explorer_service.views.image_proxy import fetch_article

        result = fetch_article(self.article_no_pages, 100 * 1024 * 1024)
        self.assertIsNone(result)

    @patch('scan_explorer_service.views.image_proxy.fetch_object')
    def test_thumbnail_empty_collection_returns_400(self, mock_fetch_object):
        """Verifies that thumbnail endpoint returns 400 for a collection with no pages."""
        response = self.client.get(url_for('proxy.image_proxy_thumbnail',
                                           id=self.collection.id, type='collection'))
        self.assertEqual(response.status_code, 400)


if __name__ == '__main__':
    unittest.main()
