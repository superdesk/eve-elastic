# -*- coding: utf-8 -*-

from unittest import TestCase
from datetime import datetime
from .elastic import parse_date

class TestElasticValidator(TestCase):
    pass

class TestElasticDriver(TestCase):

    def test_parse_date(self):
        date = parse_date('2013-11-06T07:56:01.414944+00:00')
        self.assertIsInstance(date, datetime)
        self.assertEquals('07:56+0000', date.strftime('%H:%M%z'))
