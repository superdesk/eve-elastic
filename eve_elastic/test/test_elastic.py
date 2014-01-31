# -*- coding: utf-8 -*-

import eve
from unittest import TestCase
from datetime import datetime
from flask import json
from eve.utils import config, ParsedRequest
from ..elastic import parse_date, Elastic


DOMAIN = {
    'items': {
        'schema': {
            'uri': {'type': 'string', 'unique': True},
            'name': {'type': 'string'},
            'firstcreated': {'type': 'datetime'},
        },
        'datasource': {
            'backend': 'elastic',
            'projection': {'firstcreated': 1}
        }
    }
}


class TestElastic(TestCase):

    def setUp(self):
        settings = {'DOMAIN': DOMAIN}
        settings['ELASTICSEARCH_URL'] = 'http://localhost:9200'
        settings['ELASTICSEARCH_INDEX'] = 'elastic_tests'
        self.app = eve.Eve(settings=settings, data=Elastic)
        with self.app.test_request_context():
            self.app.data.remove('items')

    def test_parse_date(self):
        date = parse_date('2013-11-06T07:56:01.414944+00:00')
        self.assertIsInstance(date, datetime)
        self.assertEquals('07:56+0000', date.strftime('%H:%M%z'))

    def test_put_mapping(self):
        elastic = Elastic(None)
        elastic.init_app(self.app)
        elastic.put_mapping(self.app)

        mapping = elastic.es.get_mapping(elastic.index)[elastic.index]
        items_mapping = mapping['items']['properties']

        self.assertIn('firstcreated', items_mapping)
        self.assertEquals('date', items_mapping['firstcreated']['type'])

        self.assertIn(config.DATE_CREATED, items_mapping)
        self.assertIn(config.LAST_UPDATED, items_mapping)

        self.assertIn('uri', items_mapping)

    def test_dates_are_parsed_on_fetch(self):
        with self.app.test_request_context():
            self.app.data.insert('items', [{'uri': 'test', 'firstcreated': '2012-10-10T11:12:13+0000'}])
            item = self.app.data.find_one('items', uri='test')
            self.assertIsInstance(item['firstcreated'], datetime)

    def test_query_filter_with_filter_dsl(self):
        with self.app.test_request_context():
            self.app.data.insert('items', [
                {'uri': 'u1', 'name': 'foo', 'firstcreated': '2012-01-01T11:12:13+0000'},
                {'uri': 'u2', 'name': 'bar', 'firstcreated': '2013-01-01T11:12:13+0000'},
            ])

        query_filter = {
            'term': {'name': 'foo'}
        }

        with self.app.test_request_context('?filter=' + json.dumps(query_filter)):
            req = ParsedRequest()
            self.assertEquals(1, self.app.data.find('items', req, None).count())

        with self.app.test_request_context('?q=bar&filter=' + json.dumps(query_filter)):
            req = ParsedRequest()
            self.assertEquals(0, self.app.data.find('items', req, None).count())
