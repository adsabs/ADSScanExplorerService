from flask import url_for, jsonify
import unittest
from unittest.mock import patch
from scan_explorer_service.models import Collection, Page, Article
from scan_explorer_service.tests.base import TestCaseDatabase
from scan_explorer_service.models import Base
import json
import opensearchpy

class TestMetadata(TestCaseDatabase):

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
        self.collection = Collection(type = 'type', journal = 'journal', volume = 'volume')
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

        self.page = Page(name = 'page', collection_id = self.collection.id)
        self.page.width = 1000
        self.page.height = 1000
        self.page.label = 'label'
        self.page.volume_running_page_num = 100
        self.page.color_type = 'BW'
        self.page.page_type = 'Normal'
        self.page_text = 'Some random ocr text'

        self.page_json = {'name':self.page.name, 'format':self.page.format, 'color_type':self.page.color_type, 'page_type':self.page.page_type, 'label':self.page.label,
         'width':self.page.width, 'height':self.page.height, 'collection_id':self.page.collection_id, 'volume_running_page_num':self.page.volume_running_page_num + 1}

        self.article_json = {'bibcode': '1988ApJ...333..353S', 'collection_id':self.collection.id}
        self.collection_json = {'type': 'new_type', 'journal':self.collection.journal, 'volume':self.collection.volume, 'pages':[self.page_json]}
        
        self.app.db.session.add(self.page)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.page)

        self.article.pages.append(self.page)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article)

        self.article2.pages.append(self.page)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article2)
        
        # Serialize here since lazily loaded pages will disassociate with session
        self.article_serialized = self.article.serialized
        self.article2_serialized = self.article2.serialized

        self.open_search_page_response = {"hits":{"total":{"value":1,"relation":"eq"},"max_score":None,"hits":[{'_source':{'page_id':self.page.id, 'volume_id':self.page.collection_id, 'page_label':self.page.label, 'page_number': self.page.volume_running_page_num}}]}}
        self.open_search_article_response = {"hits":{"total":{"value":1,"relation":"eq"},"max_score":None,"hits":[]},"aggregations":{"total_count":{"value":1},"ids":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":self.article.id,"doc_count":3,"min_page":{"value":1.0}}]}}}
        self.open_search_volume_response = {"hits":{"total":{"value":1,"relation":"eq"},"max_score":None,"hits":[]},"aggregations":{"total_count":{"value":1},"ids":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":self.collection.id,"doc_count":1}]}}}
        self.open_search_article_nohit_response = {"hits":{"total":{"value":0,"relation":"eq"},"max_score":None,"hits":[]},"aggregations":{"total_count":{"value":0},"ids":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[]}}}
        self.open_search_ocr_response = {"hits":{"total":{"value":1,"relation":"eq"},"max_score":None,"hits":[{'_source':{'text':self.page_text}}]}}

    @patch('opensearchpy.OpenSearch')
    def test_get_article(self, OpenSearch):
        es = OpenSearch.return_value
        es.search.return_value = self.open_search_article_response

        # Fetch     
        url = url_for("metadata.article_search", q='bibcode:' + self.article.bibcode, page=1, limit = 10)
        r = self.client.get(url)
        expected_query = {'query': {'query_string': {'query': 'article_bibcodes_lowercase:1988ApJ...333..341R', 'fields': ['article_bibcodes', 'journal', 'volume_id_lowercase', 'volume'], 'default_operator': 'AND'}}, 'size': 0, 'aggs': {'total_count': {'cardinality': {'field': 'article_bibcodes'}}, 'ids': {'terms': {'field': 'article_bibcodes', 'size': 10000}, 'aggs': {'bucket_sort': {'bucket_sort': {'sort': [{'_key': {'order': 'desc'}}], 'size': 10, 'from': 0}}}}}}
        call_args, call_kwargs = es.search.call_args
        self.assertEqual(expected_query, call_kwargs.get('body'))
        self.assertStatus(r, 200)
        expected_response = {"extra_collection_count": 0, "extra_page_count": 0,  "items": [{"bibcode": self.article.bibcode, "id": self.article.id, "pages": 3 }], "limit": 10, "page": 1, "pageCount": 1, "query": "",  "total": 1}
        self.assertEqual(r.data, jsonify(expected_response).data)

    @patch('opensearchpy.OpenSearch')
    def test_get_collection(self, OpenSearch):
        es = OpenSearch.return_value
        es.search.return_value = self.open_search_volume_response

        # Fetch     
        url = url_for("metadata.collection_search", q='bibstem:' + self.collection.id, page=1, limit = 10)
        r = self.client.get(url)
        expected_query = {'query': {'query_string': {'query': 'journal:journalvolume', 'fields': ['article_bibcodes', 'journal', 'volume_id_lowercase', 'volume'], 'default_operator': 'AND'}}, 'size': 0, 'aggs': {'total_count': {'cardinality': {'field': 'volume_id'}}, 'ids': {'terms': {'field': 'volume_id', 'size': 10000}, 'aggs': {'bucket_sort': {'bucket_sort': {'sort': [{'_key': {'order': 'desc'}}], 'size': 10, 'from': 0}}}}}}
        call_args, call_kwargs = es.search.call_args
        print(call_kwargs.get('body'))
        self.assertEqual(expected_query, call_kwargs.get('body'))
        self.assertStatus(r, 200)
        expected_response = {"items": [{"id": self.collection.id ,"journal": "journ", "pages": 1, 'volume':'alvo' }], "limit": 10, "page": 1, "pageCount": 1, "query": "",  "total": 1}
        self.assertEqual(r.data, jsonify(expected_response).data)

    @patch('opensearchpy.OpenSearch')
    def test_get_page(self, OpenSearch):
        es = OpenSearch.return_value
        es.search.return_value = self.open_search_page_response

        # Fetch     
        url = url_for("metadata.page_search", q='full:' + '"test text"', page=1, limit = 10)
        r = self.client.get(url)
        expected_query = {'query': {'query_string': {'query': 'text:"test text"', 'fields': ['article_bibcodes', 'journal', 'volume_id_lowercase', 'volume'], 'default_operator': 'AND'}}, '_source': {'include': ['page_id', 'volume_id', 'page_label', 'page_number']}, 'size': 10, 'from': 0, 'track_total_hits': True, 'sort': [{'article_bibcodes': {'order': 'desc'}}, {'page_number': {'order': 'asc'}}]}
        call_args, call_kwargs = es.search.call_args
        self.assertEqual(expected_query, call_kwargs.get('body'))
        self.assertStatus(r, 200)
        expected_response = {"items": [{"id": self.page.id ,"journal": "journ", 'label': self.page.label, "volume_page_num": self.page.volume_running_page_num, 'volume':'alvo', 'collection_id': self.collection.id }], "limit": 10, "page": 1, "pageCount": 1, "query": "test text",  "total": 1}

        self.assertEqual(str(r.data), str(jsonify(expected_response).data))

    def test_query_parsing_failures(self):
        url = url_for("metadata.article_search", q='')
        r = self.client.get(url)
        self.assertStatus(r, 400)
 
        url = url_for("metadata.article_search", q='pagetype:Wrong')
        r = self.client.get(url)
        self.assertStatus(r, 400)

        url = url_for("metadata.article_search", q='pagecolor:Wrong')
        r = self.client.get(url)
        self.assertStatus(r, 400)

        url = url_for("metadata.article_search", q='wrong:wrong')
        r = self.client.get(url)
        self.assertStatus(r, 400)

    def test_bad_pagination_is_a_client_error(self):
        """Invalid page or limit is the caller's mistake, so it must not read as an outage."""
        for endpoint in ("metadata.article_search", "metadata.collection_search", "metadata.page_search"):
            for params in ({"limit": -1}, {"limit": 0}, {"page": 0}, {"page": -3}):
                url = url_for(endpoint, q='volume:1', **params)
                r = self.client.get(url)
                self.assertStatus(r, 400, f"{endpoint} with {params}")
                self.assertIn('application/json', r.content_type)

    @patch('opensearchpy.OpenSearch')
    def test_deep_pagination_is_rejected_before_opensearch_is_called(self, OpenSearch):
        """Paging past the result window is the caller's mistake, not a backend outage."""
        window = self.app.config.get('OPEN_SEARCH_MAX_RESULT_WINDOW', 10000)
        limit = 5
        url = url_for("metadata.page_search", q='volume:1', page=window // limit + 1, limit=limit)
        r = self.client.get(url)
        self.assertStatus(r, 400)
        self.assertIn('searchable window', json.loads(r.data)['message'])
        OpenSearch.return_value.search.assert_not_called()

    @patch('opensearchpy.OpenSearch')
    def test_the_last_advertised_page_is_reachable(self, OpenSearch):
        """pageCount must never name a page the result-window guard would reject."""
        window = self.app.config.get('OPEN_SEARCH_MAX_RESULT_WINDOW', 10000)
        limit = 3
        OpenSearch.return_value.search.return_value = {
            "hits": {"total": {"value": 50000, "relation": "eq"}, "max_score": None, "hits": []},
            "aggregations": {"total_count": {"value": 50000}, "ids": {"buckets": []}}}

        r = self.client.get(url_for("metadata.page_search", q='volume:1', page=1, limit=limit))
        self.assertStatus(r, 200)
        advertised = json.loads(r.data)['pageCount']
        self.assertLessEqual(advertised * limit, window,
                             'the advertised final page must sit inside the window')

    @patch('opensearchpy.OpenSearch')
    def test_the_page_exactly_on_the_window_boundary_is_allowed(self, OpenSearch):
        window = self.app.config.get('OPEN_SEARCH_MAX_RESULT_WINDOW', 10000)
        limit = 5
        OpenSearch.return_value.search.return_value = {
            "hits": {"total": {"value": 50000, "relation": "eq"}, "max_score": None, "hits": []}}

        url = url_for("metadata.page_search", q='volume:1', page=window // limit, limit=limit)
        self.assertStatus(self.client.get(url), 200)

    @patch('opensearchpy.OpenSearch')
    def test_non_numeric_pagination_falls_back_to_the_default(self, OpenSearch):
        """Deliberate: a page or limit that is not a number is served as the default, not rejected."""
        OpenSearch.return_value.search.return_value = {
            "hits": {"total": {"value": 0, "relation": "eq"}, "max_score": None, "hits": []},
            "aggregations": {"total_count": {"value": 0}, "ids": {"buckets": []}}}

        for params in ({"page": "abc"}, {"limit": "abc"}, {"page": "1.5"}):
            r = self.client.get(url_for("metadata.article_search", q='volume:1', **params))
            self.assertStatus(r, 200, str(params))

    def test_a_limit_beyond_the_result_window_is_a_client_error(self):
        """Without this bound, article_search returns 400 or 200 for one request depending on data."""
        window = self.app.config.get('OPEN_SEARCH_MAX_RESULT_WINDOW', 10000)
        r = self.client.get(url_for("metadata.article_search", q='volume:1', limit=window + 1))
        self.assertStatus(r, 400)

    @patch('opensearchpy.OpenSearch')
    def test_deep_article_pages_are_not_capped_when_nothing_matches(self, OpenSearch):
        """article_search probes the page index for a count; that probe must not cap the caller."""
        OpenSearch.return_value.search.return_value = {
            "hits": {"total": {"value": 0, "relation": "eq"}, "max_score": None, "hits": []},
            "aggregations": {"total_count": {"value": 0}, "ids": {"buckets": []}}}

        url = url_for("metadata.article_search", q='volume:1', page=2001, limit=5)
        self.assertStatus(self.client.get(url), 200)

    SEARCH_ENDPOINTS = ("metadata.article_search", "metadata.collection_search", "metadata.page_search")

    @patch('opensearchpy.OpenSearch')
    def test_search_outage_is_reported_as_503(self, OpenSearch):
        """Every search endpoint must report an unreachable backend as an outage."""
        OpenSearch.return_value.search.side_effect = opensearchpy.exceptions.ConnectionError(
            'N/A', 'connection refused', Exception('refused'))
        for endpoint in self.SEARCH_ENDPOINTS:
            r = self.client.get(url_for(endpoint, q='volume:1'))
            self.assertStatus(r, 503, endpoint)
            self.assertIn('unavailable', json.loads(r.data)['message'].lower())

    @patch('opensearchpy.OpenSearch')
    def test_search_misconfiguration_is_ours_not_an_outage(self, OpenSearch):
        """A missing index is a 500, so alerting sees it and nobody reads it as 'OpenSearch is down'."""
        OpenSearch.return_value.search.side_effect = opensearchpy.exceptions.NotFoundError(
            404, 'index_not_found_exception', {'error': 'no such index'})
        for endpoint in self.SEARCH_ENDPOINTS:
            r = self.client.get(url_for(endpoint, q='volume:1'))
            self.assertStatus(r, 500, endpoint)
            self.assertNotIn('no such index', r.data.decode())

    @patch('opensearchpy.OpenSearch')
    def test_search_internal_failure_does_not_leak_its_text(self, OpenSearch):
        OpenSearch.return_value.search.side_effect = RuntimeError('could not connect to db.internal')
        for endpoint in self.SEARCH_ENDPOINTS:
            r = self.client.get(url_for(endpoint, q='volume:1'))
            self.assertStatus(r, 500, endpoint)
            self.assertIn('application/json', r.content_type)
            self.assertNotIn('db.internal', r.data.decode())

    @patch('opensearchpy.OpenSearch')
    def test_search_rejected_query_is_a_client_error(self, OpenSearch):
        OpenSearch.return_value.search.side_effect = opensearchpy.exceptions.RequestError(
            400, 'search_phase_execution_exception', {'error': 'bad query'})
        for endpoint in self.SEARCH_ENDPOINTS:
            r = self.client.get(url_for(endpoint, q='volume:1'))
            self.assertStatus(r, 400, endpoint)

    @patch('opensearchpy.OpenSearch')
    def test_ocr_failure_follows_the_same_contract(self, OpenSearch):
        OpenSearch.return_value.search.side_effect = RuntimeError('could not connect to db.internal')
        r = self.client.get(url_for("metadata.get_page_ocr", id=self.article.id, page_number=1))
        self.assertStatus(r, 500)
        self.assertNotIn('db.internal', r.data.decode())

    @patch('opensearchpy.OpenSearch')
    def test_query_parsing_sucess(self, OpenSearch):
        es = OpenSearch.return_value
        es.search.return_value = self.open_search_article_nohit_response
        url = url_for("metadata.article_search", q='bibcode:1 bibstem:2 full:3 page_sequence:4 page:5 pagetype:Normal pagecolor:BW project:"PHaEDRA" volume:6')
        r = self.client.get(url)
        self.assertStatus(r, 200)

        url = url_for('metadata.article_search', q='pagetype:normal pagecolor:bw project:"historical literature"')
        r = self.client.get(url)
        self.assertStatus(r, 200)

    @patch('opensearchpy.OpenSearch')
    def test_get_ocr(self, OpenSearch):
        es = OpenSearch.return_value
        es.search.return_value = self.open_search_ocr_response
        article_id = self.article.id
        url = url_for("metadata.get_page_ocr", id=article_id)
        r = self.client.get(url)
        self.assertStatus(r, 200)
        self.assertEqual(r.data, b'Some random ocr text')

    def test_put_page(self):
        url = url_for("metadata.put_page")
        r = self.client.put(url, json=self.page_json)
        self.assertStatus(r,200)

        for page in self.app.db.session.query(Page).all():
            self.assertEqual(page.volume_running_page_num, 101)

    def test_put_article(self):
        url = url_for("metadata.put_article")
        r = self.client.put(url, json=self.article_json)
        self.assertStatus(r,200)
        
        n = 0
        for article in self.app.db.session.query(Article).all():
            n += 1
            self.assertTrue(article.bibcode in ['1988ApJ...333..341R', '1988ApJ...333..352S', '1988ApJ...333..353S'])
        self.assertEqual(n, 3)

    def test_put_collection(self):
        url = url_for("metadata.put_collection")
        r = self.client.put(url, json=self.collection_json)
        self.assertStatus(r,200)

        n = 0
        for collection in self.app.db.session.query(Collection).all():
            n += 1
            self.assertEqual(collection.type, 'new_type')
            self.assertEqual(collection.pages.first().volume_running_page_num, 101)
        self.assertEqual(n, 1)

    def test_article_collection(self):
        url = url_for("metadata.article_collection", bibcode = self.article.bibcode)
        r = self.client.get(url)
        self.assertStatus(r,200)
        data = json.loads(r.data)

        self.assertStatus(r, 200)
        self.assertEqual(data, {'id': 'journalvolume', 'selected_page': 100})

    def test_put_collection_with_articles(self):
        """put_collection bulk-inserts articles and links them to pages."""
        collection_json = {
            'type': 'type',
            'journal': self.collection.journal,
            'volume': self.collection.volume,
            'pages': [{
                'name': 'pageA',
                'color_type': 'BW',
                'page_type': 'Normal',
                'label': '1',
                'width': 100,
                'height': 100,
                'volume_running_page_num': 1,
                'articles': [{'bibcode': '2000ApJ...001..001A'}],
            }]
        }
        url = url_for("metadata.put_collection")
        r = self.client.put(url, json=collection_json)
        self.assertStatus(r, 200)

        collection_id = r.get_json()['id']
        articles = self.app.db.session.query(Article).filter(Article.collection_id == collection_id).all()
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0].bibcode, '2000ApJ...001..001A')

        pages = self.app.db.session.query(Page).filter(Page.collection_id == collection_id).all()
        self.assertEqual(len(pages), 1)

    @patch('scan_explorer_service.views.metadata.cache_delete_manifests')
    def test_put_collection_invalidates_its_articles(self, mock_delete):
        """The collection's pages changed, so every article manifest in it is now stale."""
        collection_json = {
            'type': 'type',
            'journal': self.collection.journal,
            'volume': self.collection.volume,
            'pages': [{
                'name': 'pageA',
                'color_type': 'BW',
                'page_type': 'Normal',
                'label': '1',
                'width': 100,
                'height': 100,
                'volume_running_page_num': 1,
                'articles': [{'bibcode': '2000ApJ...001..001A'}],
            }]
        }
        url = url_for("metadata.put_collection")
        self.assertStatus(self.client.put(url, json=collection_json), 200)

        mock_delete.assert_called_once()
        invalidated = set(mock_delete.call_args[0][0])
        self.assertIn(self.collection.id, invalidated)
        self.assertIn('2000ApJ...001..001A', invalidated)
        self.assertIn(self.article.id, invalidated)
        self.assertIn(self.article2.id, invalidated)

    @patch('scan_explorer_service.views.metadata.cache_delete_manifests')
    def test_put_page_invalidates_its_articles_and_collection(self, mock_delete):
        page_json = dict(self.page_json)
        page_json['articles'] = [{'bibcode': self.article.bibcode}]
        collection_id = self.collection.id
        bibcode = self.article.bibcode

        r = self.client.put(url_for("metadata.put_page"), json=page_json)
        self.assertStatus(r, 200)
        invalidated = set(mock_delete.call_args[0][0])
        self.assertIn(collection_id, invalidated)
        self.assertIn(bibcode, invalidated)

    @patch('scan_explorer_service.views.metadata.cache_delete_manifests')
    def test_put_article_invalidates_itself_and_its_collection(self, mock_delete):
        collection_id = self.collection.id
        r = self.client.put(url_for("metadata.put_article"),
                            json={'bibcode': '2001ApJ...555..555Z', 'collection_id': collection_id})
        self.assertStatus(r, 200)
        invalidated = set(mock_delete.call_args[0][0])
        self.assertIn('2001ApJ...555..555Z', invalidated)
        self.assertIn(collection_id, invalidated)

    def test_put_collection_deduplicates_articles(self):
        """An article appearing in multiple pages is inserted only once."""
        collection_json = {
            'type': 'type',
            'journal': self.collection.journal,
            'volume': self.collection.volume,
            'pages': [
                {
                    'name': 'pageA',
                    'color_type': 'BW',
                    'page_type': 'Normal',
                    'label': '1',
                    'width': 100,
                    'height': 100,
                    'volume_running_page_num': 1,
                    'articles': [{'bibcode': '2000ApJ...001..001A'}],
                },
                {
                    'name': 'pageB',
                    'color_type': 'BW',
                    'page_type': 'Normal',
                    'label': '2',
                    'width': 100,
                    'height': 100,
                    'volume_running_page_num': 2,
                    'articles': [{'bibcode': '2000ApJ...001..001A'}],
                },
            ]
        }
        url = url_for("metadata.put_collection")
        r = self.client.put(url, json=collection_json)
        self.assertStatus(r, 200)

        collection_id = r.get_json()['id']
        articles = self.app.db.session.query(Article).filter(Article.collection_id == collection_id).all()
        self.assertEqual(len(articles), 1)

        pages = self.app.db.session.query(Page).filter(Page.collection_id == collection_id).all()
        self.assertEqual(len(pages), 2)

        from scan_explorer_service.models import page_article_association_table as pat
        page_ids = [p.id for p in pages]
        links = self.app.db.session.execute(
            pat.select().where(pat.c.page_id.in_(page_ids))
        ).fetchall()
        self.assertEqual(len(links), 2)


class TestNullPageHandling(TestCaseDatabase):
    """Tests for S2, S3, S5: null dereference guards when articles/collections have no pages."""

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
            'PRESERVE_CONTEXT_ON_EXCEPTION': False
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)

        self.collection = Collection(type='type', journal='journal', volume='volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()
        self.collection_id = self.collection.id

        self.article = Article(bibcode='2000ApJ...001..001X',
                               collection_id=self.collection_id)
        self.app.db.session.add(self.article)
        self.app.db.session.commit()
        self.article_bibcode = self.article.bibcode
        self.article_id = self.article.id

    def test_collection_serialized_no_pages(self):
        """S3: Collection.serialized returns thumbnail=None when no pages exist."""
        with self.app.app_context():
            col = self.app.db.session.get(Collection, self.collection_id)
            data = col.serialized
            self.assertIsNone(data['thumbnail'])
            self.assertEqual(data['pages'], 0)

    def test_article_serialized_no_pages(self):
        """S3: Article.serialized returns thumbnail=None when no pages exist."""
        with self.app.app_context():
            art = self.app.db.session.get(Article, self.article_id)
            data = art.serialized
            self.assertIsNone(data['thumbnail'])
            self.assertEqual(data['pages'], 0)

    def test_collection_thumbnail_no_pages(self):
        """S2: collection_thumbnail raises when collection has no pages."""
        from scan_explorer_service.utils.db_utils import collection_thumbnail
        with self.app.app_context():
            with self.assertRaises(Exception) as ctx:
                collection_thumbnail(self.app.db.session, self.collection_id)
            self.assertIn('No pages found', str(ctx.exception))

    def test_article_collection_no_pages(self):
        """S5 (related): article_collection returns 404 when article has no pages."""
        url = url_for("metadata.article_collection", bibcode=self.article_bibcode)
        r = self.client.get(url)
        self.assertStatus(r, 404)
        data = json.loads(r.data)
        self.assertIn('no pages', data['message'].lower())

    @patch('opensearchpy.OpenSearch')
    def test_get_page_ocr_article_no_pages(self, OpenSearch):
        """S5: get_page_ocr returns 404 when article has no pages."""
        url = url_for("metadata.get_page_ocr", id=self.article_id)
        r = self.client.get(url)
        self.assertStatus(r, 404)
        data = json.loads(r.data)
        self.assertIn('no pages', data['message'].lower())


class TestOpenSearchHighlight(TestCaseDatabase):

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
            'PRESERVE_CONTEXT_ON_EXCEPTION': False
        })

    def setUp(self):
        Base.metadata.drop_all(bind=self.app.db.engine)
        Base.metadata.create_all(bind=self.app.db.engine)

    @patch('scan_explorer_service.open_search.es_search')
    def test_text_search_highlight_missing_highlight_field(self, mock_es_search):
        mock_es_search.return_value = {
            'hits': {
                'hits': [
                    {'_source': {'page_id': 'page1'}},
                ]
            }
        }
        from scan_explorer_service.open_search import text_search_highlight, EsFields
        results = list(text_search_highlight('test query', EsFields.volume_id, 'vol1'))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['page_id'], 'page1')
        self.assertEqual(results[0]['highlight'], [])


if __name__ == '__main__':
    unittest.main()
