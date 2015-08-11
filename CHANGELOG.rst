.. :changelog:

Changelog
---------

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
