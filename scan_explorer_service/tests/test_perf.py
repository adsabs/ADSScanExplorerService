import unittest
import json
import time
from flask import url_for
from unittest.mock import patch, MagicMock
from scan_explorer_service.tests.base import TestCaseDatabase
from scan_explorer_service.models import Article, Base, Collection, Page
from scan_explorer_service.utils.cache import cache_set_manifest, MANIFEST_CACHE_PREFIX
import scan_explorer_service.utils.cache as cache_mod


class TestManifestCache(TestCaseDatabase):
    """Tests for Redis-backed manifest caching behavior."""

    def create_app(self):
        from scan_explorer_service.app import create_app
        return create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'TESTING': True,
            'PROPAGATE_EXCEPTIONS': True,
            'TRAP_BAD_REQUEST_ERRORS': True,
            'PRESERVE_CONTEXT_ON_EXCEPTION': False
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)

        import scan_explorer_service.views.manifest as m
        m._redis_client = None

        self.collection = Collection(type='type', journal='cacheJ', volume='0099')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.collection)

        self.article = Article(bibcode='2099CacheTest..001A',
                               collection_id=self.collection.id)
        self.app.db.session.add(self.article)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article)

        self.page1 = Page(name='cp1', collection_id=self.collection.id,
                          volume_running_page_num=1)
        self.page1.width = 100
        self.page1.height = 100
        self.page1.label = '1'
        self.page2 = Page(name='cp2', collection_id=self.collection.id,
                          volume_running_page_num=2)
        self.page2.width = 100
        self.page2.height = 100
        self.page2.label = '2'
        self.app.db.session.add_all([self.page1, self.page2])
        self.app.db.session.commit()

        self.article.pages.append(self.page1)
        self.article.pages.append(self.page2)
        self.app.db.session.commit()

    def tearDown(self):
        cache_mod._redis_client = None
        self.app.db.session.remove()
        self.app.db.drop_all()

    def _mock_redis(self):
        """Create an in-memory mock Redis client with get/setex/delete and TTL support."""
        mock_r = MagicMock()
        store = {}

        def mock_get(key):
            entry = store.get(key)
            if entry is None:
                return None
            val, exp = entry
            if exp and time.monotonic() > exp:
                del store[key]
                return None
            return val

        def mock_setex(key, ttl, val):
            store[key] = (val, time.monotonic() + ttl)

        def mock_delete(key):
            store.pop(key, None)

        mock_r.get = mock_get
        mock_r.setex = mock_setex
        mock_r.delete = mock_delete
        mock_r.ping.return_value = True

        cache_mod._redis_client = mock_r
        return mock_r, store

    def test_cache_hit_returns_cached_json(self):
        """Verifies that a cached manifest is returned directly without regeneration."""
        mock_r, store = self._mock_redis()
        store[MANIFEST_CACHE_PREFIX + self.article.id] = ('{"@type":"sc:Manifest","cached":true}', time.monotonic() + 3600)

        url = url_for("manifest.get_manifest", id=self.article.id)
        r = self.client.get(url)
        self.assertStatus(r, 200)
        data = json.loads(r.data)
        self.assertTrue(data.get('cached'))

    def test_cache_hit_returns_correct_content_type(self):
        """Verifies that cached manifest responses have application/json content type."""
        mock_r, store = self._mock_redis()
        store[MANIFEST_CACHE_PREFIX + self.collection.id] = ('{"@type":"sc:Manifest"}', time.monotonic() + 3600)

        url = url_for("manifest.get_manifest", id=self.collection.id)
        r = self.client.get(url)
        self.assertStatus(r, 200)
        self.assertIn('application/json', r.content_type)

    def test_cache_miss_calls_setex(self):
        """Verifies that a cache miss triggers a setex call to store the manifest."""
        mock_r, store = self._mock_redis()
        original_setex = mock_r.setex
        setex_calls = []

        def tracking_setex(key, ttl, val):
            setex_calls.append(key)
            return original_setex(key, ttl, val)

        mock_r.setex = tracking_setex

        cache_set_manifest(self.article.id, '{"@type":"sc:Manifest"}')

        self.assertEqual(len(setex_calls), 1)
        self.assertEqual(setex_calls[0], MANIFEST_CACHE_PREFIX + self.article.id)

    def test_cached_manifest_skips_manifest_factory(self):
        """Verifies that manifest_factory is not called when the manifest is cached."""
        mock_r, store = self._mock_redis()
        store[MANIFEST_CACHE_PREFIX + self.article.id] = ('{"@type":"sc:Manifest"}', time.monotonic() + 3600)

        with patch('scan_explorer_service.views.manifest.manifest_factory') as mock_factory:
            url = url_for("manifest.get_manifest", id=self.article.id)
            r = self.client.get(url)
            self.assertStatus(r, 200)
            mock_factory.create_manifest.assert_not_called()

    def test_404_not_cached(self):
        """Verifies that 404 responses are not stored in the cache."""
        mock_r, store = self._mock_redis()

        url = url_for("manifest.get_manifest", id='nonexistent')
        r = self.client.get(url)
        self.assertStatus(r, 404)
        self.assertNotIn(MANIFEST_CACHE_PREFIX + 'nonexistent', store)

    def test_redis_unavailable_falls_through(self):
        """Verifies that the endpoint still works when Redis is unavailable."""
        cache_mod._redis_client = None

        with patch('scan_explorer_service.utils.cache.redis.from_url', side_effect=Exception("connection refused")):
            url = url_for("manifest.get_manifest", id=self.article.id)
            r = self.client.get(url)
            self.assertStatus(r, 200)
            data = json.loads(r.data)
            self.assertEqual(data['@type'], 'sc:Manifest')


class TestPdfEarlyLimitCheck(TestCaseDatabase):
    """Tests for early page-count validation before PDF generation."""

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

        self.collection = Collection(type='type', journal='journal', volume='volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()

    def test_over_limit_returns_400_immediately(self):
        """Verifies that requesting more pages than the limit returns 400 without processing."""
        response = self.client.get(url_for('proxy.pdf_save',
                                           id=self.collection.id,
                                           page_start=1,
                                           page_end=150))
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertIn('exceeds limit', data['Message'])

    def test_exactly_at_limit_passes(self):
        """Verifies that requesting exactly the page limit is allowed."""
        with patch('scan_explorer_service.views.image_proxy.fetch_images') as mock_fi, \
             patch('scan_explorer_service.views.image_proxy.img2pdf.convert') as mock_conv:
            mock_fi.return_value = [b'data']
            mock_conv.return_value = b'pdf'
            response = self.client.get(url_for('proxy.pdf_save',
                                               id=self.collection.id,
                                               page_start=1,
                                               page_end=100))
            self.assertEqual(response.status_code, 200)

    def test_one_over_limit_returns_400(self):
        """Verifies that requesting one page over the limit returns 400."""
        response = self.client.get(url_for('proxy.pdf_save',
                                           id=self.collection.id,
                                           page_start=1,
                                           page_end=101))
        self.assertEqual(response.status_code, 400)

    def test_no_page_end_passes_limit_check(self):
        """Verifies that omitting page_end bypasses the page limit check."""
        with patch('scan_explorer_service.views.image_proxy.fetch_images') as mock_fi, \
             patch('scan_explorer_service.views.image_proxy.img2pdf.convert') as mock_conv:
            mock_fi.return_value = [b'data']
            mock_conv.return_value = b'pdf'
            response = self.client.get(url_for('proxy.pdf_save',
                                               id=self.collection.id,
                                               page_start=1))
            self.assertEqual(response.status_code, 200)

    @patch('scan_explorer_service.views.image_proxy.get_item')
    def test_over_limit_does_not_touch_db(self, mock_get_item):
        """Verifies that over-limit requests are rejected before any database access."""
        response = self.client.get(url_for('proxy.pdf_save',
                                           id=self.collection.id,
                                           page_start=1,
                                           page_end=200))
        self.assertEqual(response.status_code, 400)
        mock_get_item.assert_not_called()

    def test_inverted_page_range_returns_empty_pdf(self):
        """Verifies that an inverted page range (start > end) is handled gracefully."""
        with patch('scan_explorer_service.views.image_proxy.fetch_images') as mock_fi, \
             patch('scan_explorer_service.views.image_proxy.img2pdf.convert') as mock_conv:
            mock_fi.return_value = []
            mock_conv.return_value = b'pdf'
            response = self.client.get(url_for('proxy.pdf_save',
                                               id=self.collection.id,
                                               page_start=10,
                                               page_end=5))
            self.assertIn(response.status_code, [200, 400])


class TestParallelFetchImages(TestCaseDatabase):
    """Tests for parallel image fetching used in PDF generation."""

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

        self.collection = Collection(type='type', journal='journal', volume='volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()

        self.article = Article(bibcode='1988ApJ...333..341R',
                               collection_id=self.collection.id)
        self.app.db.session.add(self.article)
        self.app.db.session.commit()

        pages = []
        for i in range(5):
            p = Page(name=f'page{i}', collection_id=self.collection.id,
                     volume_running_page_num=i + 1)
            p.width = 100
            p.height = 100
            p.label = str(i + 1)
            pages.append(p)
        self.app.db.session.add_all(pages)
        self.app.db.session.commit()

        for p in pages:
            self.article.pages.append(p)
        self.app.db.session.commit()

        self.pages = pages

    @patch('scan_explorer_service.views.image_proxy.S3Provider')
    def test_fetch_images_returns_all_pages(self, mock_s3_cls):
        """Verifies that fetch_images returns image data for all pages in the range."""
        mock_s3 = MagicMock()
        mock_s3.read_object_s3.return_value = b'image_data'
        mock_s3_cls.return_value = mock_s3

        from scan_explorer_service.views.image_proxy import fetch_images
        images = list(fetch_images(
            self.app.db.session, self.collection, 1, 5, 100,
            100 * 1024 * 1024))
        self.assertEqual(len(images), 5)
        self.assertTrue(all(img == b'image_data' for img in images))

    @patch('scan_explorer_service.views.image_proxy.S3Provider')
    def test_fetch_images_respects_memory_limit(self, mock_s3_cls):
        """Verifies that fetch_images stops fetching when the memory limit is reached."""
        mock_s3 = MagicMock()
        mock_s3.read_object_s3.return_value = b'x' * 1000
        mock_s3_cls.return_value = mock_s3

        from scan_explorer_service.views.image_proxy import fetch_images
        images = list(fetch_images(
            self.app.db.session, self.collection, 1, 5, 100,
            500))
        self.assertLess(len(images), 5)

    @patch('scan_explorer_service.views.image_proxy.S3Provider')
    def test_fetch_images_skips_none_results(self, mock_s3_cls):
        """Verifies that fetch_images filters out None results from S3."""
        mock_s3 = MagicMock()
        mock_s3.read_object_s3.side_effect = [b'data1', None, b'data3', b'data4', b'data5']
        mock_s3_cls.return_value = mock_s3

        from scan_explorer_service.views.image_proxy import fetch_images
        images = list(fetch_images(
            self.app.db.session, self.collection, 1, 5, 100,
            100 * 1024 * 1024))
        self.assertEqual(len(images), 4)

    @patch('scan_explorer_service.views.image_proxy.S3Provider')
    def test_single_s3provider_instance(self, mock_s3_cls):
        """Verifies that fetch_images reuses a single S3Provider instance across all pages."""
        mock_s3 = MagicMock()
        mock_s3.read_object_s3.return_value = b'image_data'
        mock_s3_cls.return_value = mock_s3

        from scan_explorer_service.views.image_proxy import fetch_images
        list(fetch_images(
            self.app.db.session, self.collection, 1, 5, 100,
            100 * 1024 * 1024))
        self.assertEqual(mock_s3_cls.call_count, 1)


if __name__ == '__main__':
    unittest.main()
