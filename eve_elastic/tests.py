# -*- coding: utf-8 -*-

import eve
from unittest import TestCase
from datetime import datetime
from .elastic import parse_date, Elastic
from eve.utils import config

class TestElasticValidator(TestCase):
    pass

class TestElasticDriver(TestCase):

    domain = {
        'items': {
            'scheme': {
                'uri': {'type': 'string', 'unique': True},
                'name': {'type': 'string'},
                'firstcreated': {'type': 'datetime'},
            }
        }
    }

    def setUp(self):
        settings = {'DOMAIN': self.domain}
        settings['ELASTICSEARCH_URL'] = 'http://localhost:9200'
        settings['ELASTICSEARCH_INDEX'] = 'elastic_tests'
        self.app = eve.Eve(settings=settings)

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
