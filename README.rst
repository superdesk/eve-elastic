Eve-Elastic
===========

.. image:: https://travis-ci.org/petrjasek/eve-elastic.png?branch=master
        :target: https://travis-ci.org/petrjasek/eve-elastic

Eve-Elastic is `elasticsearch <http://www.elasticsearch.org>`_ data layer for `eve REST framework <http://python-eve.org>`_.

Features
--------

- fulltext search
- filtering via elasticsearch filter dsl
- facets support
- elasticsearch mapping generator for schema

License
-------
Eve-Elastic is `GPLv3 <http://www.gnu.org/licenses/gpl-3.0.txt>`_ licensed.

Install
-------

.. code-block:: bash

    $ pip install Eve-Elastic

Usage
-----
Set elastic as your eve data layer.

.. code-block:: python

    import eve
    form eve_elastic import Elastic

    app = eve.Eve(data=Elastic)

Config
------
There are 2 options for Eve-Elastic taken from ``app.config``:

- ``ELASTICSEARCH_URL`` (default: ``'http://localhost:9200/'``)
- ``ELASTICSEARCH_INDEX`` - (default: ``'eve'``)

Query params
------------
Eve-Elastic supports eve like queries via ``where`` param which work as `term <http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-term-filter.html>`_ filter.

On top of this, there is a predefined `query_string <http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html>`_ query which does fulltext search.

.. code-block:: console

    $ curl http://localhost:5000/items?q=foo&df=name

- ``q`` - query (default: ``*``)
- ``df`` - default field (default: ``_all``)

Filter DSL
~~~~~~~~~~
For more sophisticated filtering, you can use ``filter`` query param which will be used as filter for the query, using elastic `filter dsl <http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-filters.html>`_.

Facets
------
To add a facets support for specific resource, add ``facets`` into its ``datasource``:

.. code-block:: python

    DOMAIN = {
        'contacts': {
            'datasource':
                'facets': {
                    'urgency': {'terms': {'field': 'urgency'}},
                    'versioncreated': {'date_histogram': {'field': 'versioncreated', 'interval': 'hour'}}
                }
            }
        }

You will find `more info about facets in elasticsearch docs <http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-facets.html>`_.
