import unittest
import json
import sys
import redis as redis_lib
from flask import url_for
from unittest.mock import patch, MagicMock
from werkzeug.datastructures import ImmutableMultiDict
from scan_explorer_service.tests.base import TestCaseDatabase
from scan_explorer_service.models import Base, Collection, Page, Article
from scan_explorer_service.views.image_proxy import fetch_images
from scan_explorer_service.views.metadata import _make_search_cache_key
import scan_explorer_service.utils.cache as cache_mod


class TestRedisReconnection(TestCaseDatabase):
    """Verify Redis client resets on ConnectionError and automatically reconnects on next call."""

    def create_app(self):
        from scan_explorer_service.app import create_app
        return create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'TESTING': True,
            'PROPAGATE_EXCEPTIONS': True,
            'TRAP_BAD_REQUEST_ERRORS': True,
            'PRESERVE_CONTEXT_ON_EXCEPTION': False,
            'REDIS_URL': 'redis://localhost:6379/15',
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)
        m = cache_mod
        m._redis_client = None

    def tearDown(self):
        m = cache_mod
        m._redis_client = None
        super().tearDown()

    @patch('scan_explorer_service.utils.cache.redis.from_url')
    def test_cache_get_resets_on_connection_error(self, mock_from_url):
        """Verify _redis_client is set to None after a ConnectionError so next call reconnects."""
        m = cache_mod
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_from_url.return_value = mock_client

        m.cache_get_manifest('test')
        self.assertIsNotNone(m._redis_client)

        mock_client.get.side_effect = redis_lib.ConnectionError("connection lost")
        result = m.cache_get_manifest('test')
        self.assertIsNone(result)
        self.assertIsNone(m._redis_client)

    @patch('scan_explorer_service.utils.cache.redis.from_url')
    def test_cache_set_resets_on_connection_error(self, mock_from_url):
        """Verify _redis_client resets on ConnectionError during cache writes too."""
        m = cache_mod
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_from_url.return_value = mock_client

        m.cache_set_manifest('test', '{}')
        self.assertIsNotNone(m._redis_client)

        mock_client.setex.side_effect = redis_lib.ConnectionError("connection lost")
        m.cache_set_manifest('test', '{}')
        self.assertIsNone(m._redis_client)

    @patch('scan_explorer_service.utils.cache.redis.from_url')
    def test_reconnects_after_reset(self, mock_from_url):
        """Verify a new Redis client is created after a previous connection was reset."""
        m = cache_mod
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_from_url.return_value = mock_client

        m.cache_get_manifest('test')
        mock_client.get.side_effect = redis_lib.ConnectionError("lost")
        m.cache_get_manifest('test')
        self.assertIsNone(m._redis_client)

        mock_client2 = MagicMock()
        mock_client2.ping.return_value = True
        mock_client2.get.return_value = '{"cached": true}'
        mock_from_url.return_value = mock_client2

        result = m.cache_get_manifest('test')
        self.assertEqual(result, '{"cached": true}')
        self.assertIsNotNone(m._redis_client)

    @patch('scan_explorer_service.utils.cache.redis.from_url')
    def test_get_redis_uses_lock(self, mock_from_url):
        """Verify from_url is only called once even with multiple _get_redis calls (singleton pattern)."""
        m = cache_mod
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_from_url.return_value = mock_client

        m._get_redis()
        m._get_redis()
        self.assertEqual(mock_from_url.call_count, 1)


class TestManifestCaching(TestCaseDatabase):
    """Verify manifest and search results are cached in Redis and served on subsequent requests."""

    def create_app(self):
        from scan_explorer_service.app import create_app
        return create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'TESTING': True,
            'PROPAGATE_EXCEPTIONS': True,
            'TRAP_BAD_REQUEST_ERRORS': True,
            'PRESERVE_CONTEXT_ON_EXCEPTION': False,
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)
        m = cache_mod
        m._redis_client = None

        self.collection = Collection(type='type', journal='journal', volume='volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.collection)

        self.article = Article(bibcode='1988ApJ...333..341R',
                               collection_id=self.collection.id)
        self.app.db.session.add(self.article)
        self.app.db.session.commit()

        self.page = Page(name='page', collection_id=self.collection.id)
        self.page.width = 1000
        self.page.height = 1000
        self.page.label = 'label'
        self.app.db.session.add(self.page)
        self.app.db.session.commit()

        self.article.pages.append(self.page)
        self.app.db.session.commit()

    def tearDown(self):
        m = cache_mod
        m._redis_client = None
        super().tearDown()

    @patch('scan_explorer_service.views.manifest.cache_set_manifest')
    @patch('scan_explorer_service.views.manifest.cache_get_manifest')
    def test_manifest_serves_from_cache(self, mock_get, mock_set):
        """Verify manifest is generated on first request, then served from cache on second."""
        mock_get.return_value = None
        url = url_for("manifest.get_manifest", id=self.article.id)
        r1 = self.client.get(url)
        self.assertStatus(r1, 200)
        mock_set.assert_called_once()
        cached_json = mock_set.call_args[0][1]

        mock_get.return_value = cached_json
        r2 = self.client.get(url)
        self.assertStatus(r2, 200)
        self.assertEqual(r2.content_type, 'application/json')

    @patch('scan_explorer_service.views.manifest.cache_set_search')
    @patch('scan_explorer_service.views.manifest.cache_get_search')
    @patch('opensearchpy.OpenSearch')
    def test_search_cache_key_is_hashed(self, OpenSearch, mock_get, mock_set):
        """Verify search cache keys are 32-char hex MD5 hashes, not raw query strings."""
        es = OpenSearch.return_value
        es.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": [
                {'_source': {'page_id': self.page.id, 'volume_id': self.collection.id,
                             'page_label': 'label', 'page_number': 1},
                 'highlight': {'text': ['some text']}}
            ]}
        }
        mock_get.return_value = None

        url = url_for("manifest.search", id=self.article.id, q='test query')
        self.client.get(url)

        cache_key = mock_set.call_args[0][0]
        self.assertEqual(len(cache_key), 32)
        self.assertTrue(all(c in '0123456789abcdef' for c in cache_key))


class TestFetchImagesMemoryLimit(TestCaseDatabase):
    """Verify fetch_images respects memory_limit and stops yielding when exceeded."""

    def create_app(self):
        from scan_explorer_service.app import create_app
        return create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'TESTING': True,
            'PROPAGATE_EXCEPTIONS': True,
            'TRAP_BAD_REQUEST_ERRORS': True,
            'PRESERVE_CONTEXT_ON_EXCEPTION': False,
            'IMAGE_PDF_MEMORY_LIMIT': 50,
            'IMAGE_PDF_PAGE_LIMIT': 100,
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)

        self.collection = Collection(type='type', journal='journal', volume='volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()

        for i in range(5):
            p = Page(name=f'page{i}', collection_id=self.collection.id, volume_running_page_num=i+1)
            p.width = 100
            p.height = 100
            p.label = str(i+1)
            self.app.db.session.add(p)
        self.app.db.session.commit()

    @patch('scan_explorer_service.views.image_proxy.S3Provider')
    def test_memory_limit_stops_yielding(self, mock_s3_cls):
        """Verify fetch_images stops yielding once cumulative image size exceeds memory_limit."""
        chunk = b'x' * 30
        chunk_size = sys.getsizeof(chunk)

        mock_s3 = MagicMock()
        mock_s3.read_object_s3.return_value = chunk
        mock_s3_cls.return_value = mock_s3

        memory_limit = chunk_size * 2 + 1
        images = list(fetch_images(
            self.app.db.session, self.collection, 1, 5, 100, memory_limit))
        self.assertEqual(len(images), 2)
        self.assertTrue(mock_s3.read_object_s3.call_count <= 5)


class TestPdfEarlyLimitCheck(TestCaseDatabase):
    """PDF over-limit returns 400 without hitting DB."""

    def create_app(self):
        from scan_explorer_service.app import create_app
        return create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url,
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

    def test_over_limit_returns_400(self):
        """Verify requesting more pages than IMAGE_PDF_PAGE_LIMIT returns 400 before hitting DB."""
        url = url_for('proxy.pdf_save', id='anything', page_start=1, page_end=200)
        r = self.client.get(url)
        self.assertStatus(r, 400)
        data = json.loads(r.data)
        self.assertIn('exceeds limit', data['Message'])

    def test_missing_id_returns_400(self):
        """Verify missing 'id' parameter returns 400."""
        url = url_for('proxy.pdf_save', page_start=1, page_end=5)
        r = self.client.get(url)
        self.assertStatus(r, 400)


class TestSearchValidationBeforeCache(TestCaseDatabase):
    """Verify invalid queries are rejected before any Redis cache lookup occurs."""

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
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)

    @patch('scan_explorer_service.views.metadata.cache_get_search')
    def test_empty_query_returns_400_without_cache_lookup(self, mock_cache_get):
        """Verify empty search query is rejected before any cache lookup occurs."""
        url = url_for("metadata.article_search", q='')
        r = self.client.get(url)
        self.assertStatus(r, 400)
        mock_cache_get.assert_not_called()

    @patch('scan_explorer_service.views.metadata.cache_get_search')
    def test_collection_empty_query_no_cache(self, mock_cache_get):
        """Same validation-before-cache check for collection search endpoint."""
        url = url_for("metadata.collection_search", q='')
        r = self.client.get(url)
        self.assertStatus(r, 400)
        mock_cache_get.assert_not_called()

    @patch('scan_explorer_service.views.metadata.cache_get_search')
    def test_page_search_empty_query_no_cache(self, mock_cache_get):
        """Same validation-before-cache check for page search endpoint."""
        url = url_for("metadata.page_search", q='')
        r = self.client.get(url)
        self.assertStatus(r, 400)
        mock_cache_get.assert_not_called()


class TestSearchCacheKeyMultiValue(TestCaseDatabase):
    """Verify cache keys are distinct when query params have multiple values for the same key."""

    def create_app(self):
        from scan_explorer_service.app import create_app
        return create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'TESTING': True,
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)

    def test_different_multi_params_produce_different_keys(self):
        """Verify multi-valued query params (e.g. field=title&field=abstract) produce distinct cache keys."""
        args1 = ImmutableMultiDict([('q', 'star'), ('field', 'title')])
        args2 = ImmutableMultiDict([('q', 'star'), ('field', 'title'), ('field', 'abstract')])

        key1 = _make_search_cache_key('test', args1)
        key2 = _make_search_cache_key('test', args2)
        self.assertNotEqual(key1, key2)


class TestOcrCaching(TestCaseDatabase):
    """Verify OCR text results are cached and served as text/plain on cache hits."""

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
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)

        self.collection = Collection(type='type', journal='journal', volume='volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()

        self.article = Article(bibcode='1988ApJ...333..341R',
                               collection_id=self.collection.id)
        self.app.db.session.add(self.article)
        self.app.db.session.commit()

        self.page = Page(name='page', collection_id=self.collection.id)
        self.page.width = 1000
        self.page.height = 1000
        self.page.label = 'label'
        self.page.volume_running_page_num = 100
        self.app.db.session.add(self.page)
        self.app.db.session.commit()
        self.article.pages.append(self.page)
        self.app.db.session.commit()

    @patch('scan_explorer_service.views.metadata.cache_set_search')
    @patch('scan_explorer_service.views.metadata.cache_get_search')
    @patch('opensearchpy.OpenSearch')
    def test_ocr_result_is_cached(self, OpenSearch, mock_cache_get, mock_cache_set):
        """Verify OCR text is stored in cache after first fetch and returned as text/plain."""
        es = OpenSearch.return_value
        es.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": [
                {"_source": {"text": "Some OCR text here"}}
            ]}
        }
        mock_cache_get.return_value = None

        url = url_for("metadata.get_page_ocr", id=self.article.id)
        r = self.client.get(url)
        self.assertStatus(r, 200)
        self.assertEqual(r.data, b'Some OCR text here')
        self.assertIn('text/plain', r.content_type)
        mock_cache_set.assert_called_once()
        self.assertEqual(mock_cache_set.call_args[0][1], 'Some OCR text here')

    @patch('scan_explorer_service.views.metadata.cache_get_search')
    def test_ocr_served_from_cache(self, mock_cache_get):
        """Verify cached OCR text is served directly without hitting OpenSearch."""
        mock_cache_get.return_value = 'Cached OCR text'

        url = url_for("metadata.get_page_ocr", id=self.article.id)
        r = self.client.get(url)
        self.assertStatus(r, 200)
        self.assertEqual(r.data, b'Cached OCR text')
        self.assertIn('text/plain', r.content_type)


if __name__ == '__main__':
    unittest.main()
