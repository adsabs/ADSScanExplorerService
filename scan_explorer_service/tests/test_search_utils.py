import unittest
from scan_explorer_service.app import create_app
from scan_explorer_service.tests.base import TestCaseDatabase
from scan_explorer_service.utils.search_utils import parse_query_string


class TestSearchUtils(TestCaseDatabase):
    '''Tests s'''

    def create_app(self):
        '''Start the wsgi application'''
        a = create_app(**{
            'SQLALCHEMY_DATABASE_URI': self.postgresql_url
        })
        return a

    def test_parse_query(self):
        '''Tests parsing of queries'''
        final_query, _ = parse_query_string('apj 333')
        self.assertEqual(final_query, '(apj) (333)')

        final_query, _ = parse_query_string('apj 333 full:blabla')
        self.assertEqual(final_query, '(apj) (333) text:blabla')

        final_query, _ = parse_query_string('apj 333 full:"blabla bla"')
        self.assertEqual(final_query, '(apj) (333) text:"blabla bla"')

        final_query, _ = parse_query_string('apj AND 333 full:blabla')
        self.assertEqual(final_query, '(apj) AND (333) text:blabla')

        final_query, _ = parse_query_string('volume:1 OR volume:2')
        self.assertEqual(final_query, 'volume_int:1 OR volume_int:2')

        final_query, _ = parse_query_string('volume:[1 TO 5]')
        self.assertEqual(final_query, 'volume_int:[1 TO 5]')

        final_query, _ = parse_query_string('PageColor:grAYsCaLe')
        self.assertEqual(final_query, 'page_color:Grayscale')

        final_query, _ = parse_query_string('PageColor:BW')
        self.assertEqual(final_query, 'page_color:BW')

        final_query, _ = parse_query_string('PageColor:cOlor')
        self.assertEqual(final_query, 'page_color:Color')


if __name__ == '__main__':
    unittest.main()
