.. :changelog:

Changelog
---------

7.1.2 (2020-10-05)
++++++++++++++++++

- use default search params in find method.

7.1.1 (2020-09-18)
++++++++++++++++++

- fix broken aggregation query.

7.1.0 (2020-08-03)
++++++++++++++++++

- add config to control total hits `ELASTICSEARCH_TRACK_TOTAL_HITS`
- make it possible to override `doc_type` on insert (by Luis Fernando Gomes)

7.0.8 (2020-07-28)
++++++++++++++++++

- fix nested filter in sort context

7.0.7 (2020-06-18)
++++++++++++++++++

- fix error when highlights are enabled but no query string query is used

7.0.6 (2020-06-03)
++++++++++++++++++

- fix find_one with query lookup

7.0.5 (2020-04-15)
++++++++++++++++++

- fix filtered query handling

7.0.4 (2020-04-07)
++++++++++++++++++

- fix bulk insert with `version` in doc

7.0.3 (2019-09-16)
++++++++++++++++++

- handle `not` query with `filter` inside

7.0.2 (2019-09-09)
++++++++++++++++++

- fix empty top level filter

7.0.1 (2019-08-23)
++++++++++++++++++

- fix nested queries with filter

7.0.0 (2019-08-09)
++++++++++++++++++

- initial support for elastic 7

2.5.0 (2019-01-03)
++++++++++++++++++

- parse inner hits for nested query

2.4.2 (2018-08-06)
++++++++++++++++++

- fix install issue with ciso9601 dependency (by Luis Fernando Gomes)

2.4.1 (2018-02-13)
++++++++++++++++++

- avoid closing/opening index on put_settings if settings are already set

2.4 (2017-08-02)
++++++++++++++++

- add support for kwargs to ``remove`` (by Luis Fernando Gomes)

2.3 (2017-07-27)
++++++++++++++++

- add support for parent-child relations.

2.2 (2017-07-21)
++++++++++++++++

- add `ELASTICSEARCH_RETRY_ON_CONFLICT` setting

2.1 (2017-03-27)
++++++++++++++++

- support `es_highlight` to be a callback.

2.0 (2017-02-03)
++++++++++++++++

- add support for elastic 2

0.6 (2016-11-16)
++++++++++++++++

- handle projections
- parse dates using `ciso8601`

0.5.2 (2016-11-15)
++++++++++++++++++

- make serializer configurable again

0.5 (2016-11-14)
++++++++++++++++

- allow list of urls to be set in ``ELASTICSEARCH_URL``
- add support for ``where`` param
- handle ``bson.ObjectId`` serialization

0.4.1 (2016-11-08)
++++++++++++++++++

- fix ``init_index`` on versions endpoints

0.4 (2016-11-03)
++++++++++++++++

- implement ``elastic_prefix`` resource config similar to ``mongo_prefix`` in eve
- change ``init_app`` behaviour - it won't call ``init_index`` anymore, it must be
  called explicitly

0.3.8 (2016-08-05)
++++++++++++++++++

- add search highlights config
- fix aggregations request trigger parsing

0.3.7 (2016-04-13)
++++++++++++++++++

- fix search when there is no ``request.args``

0.3.6 (2016-03-21)
++++++++++++++++++

- fix ``find_one`` with multi term lookup

0.3.5 (2016-03-17)
++++++++++++++++++

- add ``skip_index_init`` param to Elastic

0.3.4 (2016-03-17)
++++++++++++++++++

- make ``init_index`` put mapping always, no matter if index is there already

0.3.3 (2016-03-15)
++++++++++++++++++

- introduce ``init_index`` method to create index and put mapping

0.3.2 (2016-02-11)
++++++++++++++++++

- introduce ``ELASTICSEARCH_AUTO_AGGREGATIONS`` config option

0.3.1 (2016-02-11)
++++++++++++++++++

- introduce ``ELASTICSEARCH_FORCE_REFRESH`` settings that is ``True`` by default
- fix for ``ELASTICSEARCH_INDEXES`` when using ``datasource.source`` config

0.3.0 (2016-02-08)
++++++++++++++++++

- introduce ``ELASTICSEARCH_INDEXES`` settings for setting different index per resource

0.2.21 (2015-11-20)
+++++++++++++++++++

- try to put settings in case put mapping is failing

0.2.20 (2015-11-16)
+++++++++++++++++++

- make it possible to specify index settings (by Mayur Dhamanwala)

0.2.19 (2015-09-29)
+++++++++++++++++++

- use `ELASTICSEARCH_URL` value as is so that it works with auth and https

0.2.18 (2015-08-12)
+++++++++++++++++++

- throw `InvalidSearchString` exception in case elastic returns `SearchParseException` (by Mugur Rus)

0.2.17 (2015-08-11)
+++++++++++++++++++

- add support for phase search via `q` param (by Mugur Rus)

0.2.16 (2015-08-04)
+++++++++++++++++++

- fix pip install

0.2.15 (2015-08-04)
+++++++++++++++++++

- parse mapping for fields type `dict`
- avoid hidden dependencies - put it in requirements file (by Dinu Ghermany)

0.2.14 (2015-07-31)
+++++++++++++++++++

- avoid pymongo dependency

0.2.13 (2015-07-22)
+++++++++++++++++++

- add `default_operator` param to `_build_query_string` (by Mugur Rus)

0.2.12 (2015-07-07)
+++++++++++++++++++

- use both resource and datasource schema to convert datetime values (by Anca Farcas)

0.2.11 (2015-06-22)
+++++++++++++++++++

- return no hits when trying to find one resource and there is no mapping

0.2.10 (2015-06-12)
+++++++++++++++++++

- fix: make it eve 0.6 compatible

0.2.9 (2015-05-11)
++++++++++++++++++

- fix: stop converting null values to datetime

0.2.7 (2015-04-16)
++++++++++++++++++

- feat: allow delete by query

0.2.6 (2015-02-10)
++++++++++++++++++

- fix a bug when getting cursor count

0.2.5 (2015-02-09)
++++++++++++++++++

- add index param to `put_mapping` method

0.2.4 (2014-12-26)
++++++++++++++++++

- add `build_query_string` method

0.2.3 (2014-12-08)
++++++++++++++++++

- fix serializer on python 2.7 (by Jonathan Dray)

0.2.2 (2014-12-02)
++++++++++++++++++

- make use of score sort if there is a query defined

0.2.1 (2014-11-27)
++++++++++++++++++

- allow resource filters being callbacks for request specific filtering

0.2.0 (2014-11-24)
++++++++++++++++++

- fix `q` param search for using eve <= 0.4
- fix `filters` request args filtering
- let user set mapping in schema
- support aggregations 

0.1.17 (2014-11-12)
+++++++++++++++++++

- switch to elasticsearch lib
- add factory for es and indices

0.1.13 (2014-07-21)
+++++++++++++++++++

- fix `count` to allow extra params

0.1.12 (2014-07-08)
+++++++++++++++++++

- fix for superdesk `search_backend` setting

0.1.11 (2014-06-27)
+++++++++++++++++++

- add custom json serializer to work with `bson.ObjectId`

0.1.10 (2014-06-11)
+++++++++++++++++++

- fix `is_empty`

0.1.9 (2014-05-29)
++++++++++++++++++

- fix `is_empty` call
- implement `find_one_raw`

0.1.8 (2014-05-29)
++++++++++++++++++

- fix remove by lookup

0.1.7 (2014-05-28)
++++++++++++++++++

- preserve mapping after deleting all documents for given type

0.1.6 (2014-05-09)
++++++++++++++++++

- support ``source`` param on find

0.1.5 (2014-05-05)
++++++++++++++++++

- fix for elastic 1.0+ fields handling

0.1.4 (2014-05-02)
++++++++++++++++++

- make it work with elastic 1.0+

0.1.3 (2014-01-31)
++++++++++++++++++

- allow filtering via elasticsearch filter dsl

0.1.2 (2014-01-30)
++++++++++++++++++

- fix pip install (add missing MANIFEST file)

0.1.1 (2014-01-30)
++++++++++++++++++

- add changelog ;)
- migrate readme to rst and use it for ``long_description``

0.1.0 (2014-01-28)
++++++++++++++++++

- initial release
