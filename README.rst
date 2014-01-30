Eve-Elastic
===========

.. image:: https://travis-ci.org/petrjasek/eve-elastic.png?branch=master
        :target: https://travis-ci.org/petrjasek/eve-elastic

Eve-Elastic is `elasticsearch <http://www.elasticsearch.org>`_ data layer for `eve REST framework <http://python-eve.org>`_.

Features
--------

- facets support
- generate mapping for schema

License
-------

Eve-Elastic is using `GPLv3 <http://www.gnu.org/licenses/gpl-3.0.txt>`_ license.

Install
-------

.. code-block:: bash

    $ pip install Eve-Elastic

Usage
-----

.. code-block:: python

    import eve
    form eve_elastic import Elastic

    app = eve.Eve(data=Elastic)

Config
------

There are 2 options for Eve-Elastic taken from `app.config`:

- ``ELASTICSEARCH_URL`` (default: ``'http://localhost:9200/'``)
- ``ELASTICSEARCH_INDEX`` - (default: ``'eve'``)

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
