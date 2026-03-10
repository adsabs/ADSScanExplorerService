from flask import url_for
from unittest.mock import patch
import unittest
from scan_explorer_service.models import Collection, Page, Article
from scan_explorer_service.tests.base import TestCaseDatabase
from scan_explorer_service.models import Base
import json

class TestManifest(TestCaseDatabase):

    def create_app(self):
        '''Start the wsgi application'''
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

        self.collection = Collection(type = 'type', journal = 'journal', volume = 'volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.collection)

        self.article = Article(bibcode='1988ApJ...333..341R',
                               collection_id=self.collection.id)
        self.app.db.session.add(self.article)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.article)

        self.page = Page(name='page', collection_id = self.collection.id)
        self.page.width = 1000
        self.page.height = 1000
        self.page.label = 'label'
        self.app.db.session.add(self.page)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.page)

        self.article.pages.append(self.page)
        self.app.db.session.commit()
            

    def test_get_manifest(self):
        url = url_for("manifest.get_manifest", id=self.article.id)
        r = self.client.get(url)
        data = json.loads(r.data)

        self.assertStatus(r, 200)
        self.assertEqual(data['@type'], 'sc:Manifest')

    def test_get_canvas(self):
        url = url_for("manifest.get_canvas", page_id=self.page.id)
        r = self.client.get(url)
        data = json.loads(r.data)
        self.assertStatus(r, 200)
        self.assertEqual(data['@type'], 'sc:Canvas')

    @patch('opensearchpy.OpenSearch')
    def test_search_article_with_highlight(self, OpenSearch):
        open_search_highlight_response = {"hits":{"total":{"value":1,"relation":"eq"},"max_score":None,"hits":[{'_source':{'page_id':self.page.id, 'volume_id':self.page.collection_id, 'page_label':self.page.label, 'page_number': self.page.volume_running_page_num}, "highlight":{'text':'some <b>highlighted</b> text'}}]}}
        article_id = self.article.id
        es = OpenSearch.return_value
        es.search.return_value = open_search_highlight_response

        url = url_for("manifest.search", id=article_id, q='text')
        r = self.client.get(url)
        data = json.loads(r.data)
        self.assertStatus(r, 200)
        self.assertEqual(data['@type'], 'sc:AnnotationList')
        call_args, call_kwargs = es.search.call_args
        expected_query = {'query': {'bool': {'must': {'query_string': {'query': 'text article_bibcodes:' + article_id, 'default_field': 'text', 'default_operator': 'AND'}}}}, '_source': {'include': ['page_id', 'volume_id', 'page_label', 'page_number']}, 'highlight': {'fields': {'text': {}}, 'type': 'unified'}}
        self.assertEqual(expected_query, call_kwargs.get('body'))


class TestCollectionManifest(TestCaseDatabase):

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

        self.collection = Collection(type='type', journal='journal', volume='volume')
        self.app.db.session.add(self.collection)
        self.app.db.session.commit()
        self.app.db.session.refresh(self.collection)
        self.collection_id = self.collection.id

        self.article1 = Article(bibcode='1988ApJ...333..341R',
                                collection_id=self.collection_id)
        self.article2 = Article(bibcode='1988ApJ...333..352Z',
                                collection_id=self.collection_id)
        self.app.db.session.add(self.article1)
        self.app.db.session.add(self.article2)
        self.app.db.session.commit()
        self.article1_bibcode = self.article1.bibcode
        self.article2_bibcode = self.article2.bibcode

        self.page1 = Page(name='page1', collection_id=self.collection_id, volume_running_page_num=1)
        self.page1.width = 1000
        self.page1.height = 1000
        self.page1.label = '1'
        self.page2 = Page(name='page2', collection_id=self.collection_id, volume_running_page_num=2)
        self.page2.width = 1000
        self.page2.height = 1000
        self.page2.label = '2'
        self.page3 = Page(name='page3', collection_id=self.collection_id, volume_running_page_num=3)
        self.page3.width = 1000
        self.page3.height = 1000
        self.page3.label = '3'
        self.app.db.session.add_all([self.page1, self.page2, self.page3])
        self.app.db.session.commit()

        self.article1.pages.append(self.page1)
        self.article1.pages.append(self.page2)
        self.article2.pages.append(self.page2)
        self.article2.pages.append(self.page3)
        self.app.db.session.commit()

    def test_get_collection_manifest(self):
        url = url_for("manifest.get_manifest", id=self.collection_id)
        r = self.client.get(url)
        data = json.loads(r.data)

        self.assertStatus(r, 200)
        self.assertEqual(data['@type'], 'sc:Manifest')
        self.assertEqual(data['label'], self.collection_id)

        canvases = data['sequences'][0]['canvases']
        self.assertEqual(len(canvases), 3)

        ranges = data['structures']
        self.assertEqual(len(ranges), 2)
        range_labels = [r['label'] for r in ranges]
        self.assertIn(self.article1_bibcode, range_labels)
        self.assertIn(self.article2_bibcode, range_labels)

    def test_collection_manifest_page_order(self):
        url = url_for("manifest.get_manifest", id=self.collection_id)
        r = self.client.get(url)
        data = json.loads(r.data)

        canvases = data['sequences'][0]['canvases']
        labels = [c['label'] for c in canvases]
        self.assertEqual(labels, ['p. 1', 'p. 2', 'p. 3'])

    def test_collection_manifest_range_canvases(self):
        url = url_for("manifest.get_manifest", id=self.collection_id)
        r = self.client.get(url)
        data = json.loads(r.data)

        ranges = {r['label']: r for r in data['structures']}

        art1_range = ranges[self.article1_bibcode]
        self.assertEqual(len(art1_range['canvases']), 2)

        art2_range = ranges[self.article2_bibcode]
        self.assertEqual(len(art2_range['canvases']), 2)

    def test_collection_manifest_canvas_has_article_metadata(self):
        url = url_for("manifest.get_manifest", id=self.collection_id)
        r = self.client.get(url)
        data = json.loads(r.data)

        canvases = data['sequences'][0]['canvases']
        page2_canvas = canvases[1]
        metadata = {m['label']: m['value'] for m in page2_canvas['metadata']}
        self.assertIn(self.article1_bibcode, metadata['Abstract'])
        self.assertIn(self.article2_bibcode, metadata['Abstract'])

    def test_collection_manifest_not_found(self):
        url = url_for("manifest.get_manifest", id='nonexistent')
        r = self.client.get(url)
        self.assertStatus(r, 404)


class TestCanvasDictIsolation(TestCaseDatabase):
    """S1: Verify canvas_dict is reset between manifest calls on the singleton."""

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

        self.col1 = Collection(type='type', journal='jrnlA', volume='0001')
        self.col2 = Collection(type='type', journal='jrnlB', volume='0002')
        self.app.db.session.add_all([self.col1, self.col2])
        self.app.db.session.commit()

        self.art1 = Article(bibcode='2000jrnlA...1..001A', collection_id=self.col1.id)
        self.art2 = Article(bibcode='2000jrnlB...2..001B', collection_id=self.col2.id)
        self.app.db.session.add_all([self.art1, self.art2])
        self.app.db.session.commit()

        p1 = Page(name='p1', collection_id=self.col1.id, volume_running_page_num=1)
        p1.width = 100; p1.height = 100; p1.label = '1'
        p2 = Page(name='p2', collection_id=self.col1.id, volume_running_page_num=2)
        p2.width = 100; p2.height = 100; p2.label = '2'
        p3 = Page(name='p3', collection_id=self.col2.id, volume_running_page_num=1)
        p3.width = 100; p3.height = 100; p3.label = '1'
        self.app.db.session.add_all([p1, p2, p3])
        self.app.db.session.commit()

        self.art1.pages.append(p1)
        self.art1.pages.append(p2)
        self.art2.pages.append(p3)
        self.app.db.session.commit()

        self.col1_id = self.col1.id
        self.col2_id = self.col2.id

    def test_sequential_manifests_have_isolated_canvases(self):
        r1 = self.client.get(url_for("manifest.get_manifest", id=self.col1_id))
        data1 = json.loads(r1.data)
        canvases1 = data1['sequences'][0]['canvases']
        self.assertEqual(len(canvases1), 2)

        r2 = self.client.get(url_for("manifest.get_manifest", id=self.col2_id))
        data2 = json.loads(r2.data)
        canvases2 = data2['sequences'][0]['canvases']
        self.assertEqual(len(canvases2), 1)

        canvas_ids_1 = {c['@id'] for c in canvases1}
        canvas_ids_2 = {c['@id'] for c in canvases2}
        self.assertTrue(canvas_ids_1.isdisjoint(canvas_ids_2))


if __name__ == '__main__':
    unittest.main()
