# -*- coding: utf-8 -*-

import eve
from unittest import TestCase
from datetime import datetime
from flask import json
from eve.utils import config, ParsedRequest
from ..elastic import parse_date, Elastic
from bson.objectid import ObjectId


DOMAIN = {
    'items': {
        'schema': {
            'uri': {'type': 'string', 'unique': True},
            'name': {'type': 'string'},
            'firstcreated': {'type': 'datetime'},
        },
        'datasource': {
            'backend': 'elastic',
            'projection': {'firstcreated': 1, 'name': 1}
        }
    }
}


INDEX = 'elastic_tests'
DOC_TYPE = 'items'


class TestElastic(TestCase):

    def setUp(self):
        settings = {'DOMAIN': DOMAIN}
        settings['ELASTICSEARCH_URL'] = 'http://localhost:9200'
        settings['ELASTICSEARCH_INDEX'] = INDEX
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
        items_mapping = mapping['mappings']['items']['properties']

        self.assertIn('firstcreated', items_mapping)
        self.assertEquals('date', items_mapping['firstcreated']['type'])

        self.assertIn(config.DATE_CREATED, items_mapping)
        self.assertIn(config.LAST_UPDATED, items_mapping)

        self.assertIn('uri', items_mapping)

    def test_dates_are_parsed_on_fetch(self):
        with self.app.test_request_context():
            self.app.data.insert('items', [{'uri': 'test', 'firstcreated': '2012-10-10T11:12:13+0000'}])
            item = self.app.data.find_one('items', req=None, uri='test')
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

    def test_find_one_by_id(self):
        """elastic 1.0+ is using 'found' property instead of 'exists'"""
        with self.app.test_request_context():
            self.app.data.insert('items', [{'uri': 'test', config.ID_FIELD: 'testid'}])
            item = self.app.data.find_one('items', req=None, **{config.ID_FIELD: 'testid'})
            self.assertEquals('testid', item[config.ID_FIELD])

    def test_formating_fields(self):
        """when using elastic 1.0+ it puts all requested fields values into a list
        so instead of {"name": "test"} it returns {"name": ["test"]}"""
        with self.app.test_request_context():
            self.app.data.insert('items', [{'uri': 'test', 'name': 'test'}])
            item = self.app.data.find_one('items', req=None, uri='test')
            self.assertEquals('test', item['name'])

    def test_search_via_source_param(self):
        query = {'query': {'term': {'uri': 'foo'}}}
        with self.app.test_request_context('?source=' + json.dumps(query)):
            self.app.data.insert('items', [{'uri': 'foo', 'name': 'foo'}])
            self.app.data.insert('items', [{'uri': 'bar', 'name': 'bar'}])
            req = ParsedRequest()
            res = self.app.data.find('items', req, None)
            self.assertEquals(1, res.count())

    def test_mapping_is_there_after_delete(self):
        with self.app.test_request_context():
            self.app.data.put_mapping(self.app)
            mapping = self.app.data.es.get_mapping(INDEX, DOC_TYPE)
            self.app.data.remove('items')
            self.assertEquals(mapping, self.app.data.es.get_mapping(INDEX, DOC_TYPE))

    def test_find_one_raw(self):
        with self.app.test_request_context():
            ids = self.app.data.insert('items', [{'uri': 'foo', 'name': 'foo'}])
            item = self.app.data.find_one_raw('items', ids[0])
            self.assertEquals(item['name'], 'foo')

    def test_is_empty(self):
        with self.app.test_request_context():
            self.assertTrue(self.app.data.is_empty('items'))
            self.app.data.insert('items', [{'uri': 'foo'}])
            self.assertFalse(self.app.data.is_empty('items'))

    def test_storing_objectid(self):
        with self.app.test_request_context():
            res = self.app.data.insert('items', [{'uri': 'foo', 'user': ObjectId('528de7b03b80a13eefc5e610')}])
            self.assertEquals(1, len(res))
