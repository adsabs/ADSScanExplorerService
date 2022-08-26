from flask import url_for, jsonify
import unittest
from unittest.mock import patch, MagicMock
from scan_explorer_service.models import Collection, Page, Article
from scan_explorer_service.tests.base import TestCaseDatabase
from scan_explorer_service.models import Base

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

    @patch('opensearchpy.OpenSearch')
    def test_get_article(self, OpenSearch):
        es = OpenSearch.return_value
        es.search.return_value = self.open_search_article_response

        # Fetch     
        url = url_for("metadata.article_search", q='bibcode:' + self.article.bibcode, page=1, limit = 10)
        r = self.client.get(url)
        expected_query = {'query': {'bool': {'must': [{'wildcard': {'article_bibcodes_lowercase': {'value': '1988ApJ...333..341R*'}}}]}}, 'size': 0, 'aggs': {'total_count': {'cardinality': {'field': 'article_bibcodes'}}, 'ids': {'terms': {'field': 'article_bibcodes', 'size': 10000}, 'aggs': {'bucket_sort': {'bucket_sort': {'sort': [{'page_start': {'order': 'asc'}}], 'size': 10, 'from': 0}}, 'page_start': {'min': {'field': 'page_number'}}}}}}
        call_args, call_kwargs = es.search.call_args
        self.assertEqual(expected_query, call_kwargs.get('body'))
        self.assertStatus(r, 200)
        expected_response = {"items": [{"bibcode": self.article.bibcode, "id": self.article.id, "pages": 3 }], "limit": 10, "page": 1, "pageCount": 1, "query": "",  "total": 1}
        self.assertEqual(r.data, jsonify(expected_response).data)

    @patch('opensearchpy.OpenSearch')
    def test_get_collection(self, OpenSearch):
        es = OpenSearch.return_value
        es.search.return_value = self.open_search_volume_response

        # Fetch     
        url = url_for("metadata.collection_search", q='bibstem:' + self.collection.id, page=1, limit = 10)
        r = self.client.get(url)
        expected_query = {'query': {'bool': {'must': [{'wildcard': {'volume_id_lowercase': {'value': 'journalvolume*'}}}]}}, 'size': 0, 'aggs': {'total_count': {'cardinality': {'field': 'volume_id'}}, 'ids': {'terms': {'field': 'volume_id', 'size': 10000}, 'aggs': {'bucket_sort': {'bucket_sort': {'sort': [{'_count': {'order': 'desc'}}], 'size': 10, 'from': 0}}}}}}
        call_args, call_kwargs = es.search.call_args
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
        expected_query = {'query': {'bool': {'must': [{'query_string': {'default_field': 'text', 'default_operator': 'AND','query': 'test text'}}]}}, 'size': 10, 'from': 0, 'sort': [{'volume_id': {'order': 'asc'}}, {'page_number': {'order': 'asc'}}]} 
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

if __name__ == '__main__':
    unittest.main()
