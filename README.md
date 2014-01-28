## Eve-Elastic [![Build Status](https://travis-ci.org/petrjasek/eve-elastic.png?branch=master)](https://travis-ci.org/petrjasek/eve-elastic) 

Eve-Elastic is elasticsearch data layer for [eve REST framework](http://python-eve.org).

### Feature

- facets support
- generate mapping for schema

### Usage:

```python

import eve
from eve_elastic import Elastic

app = eve.Eve(data=Elastic)
```

### Configuration

There are 2 options for Eve-Elastic taken from `app.config`:

- `ELASTICSEARCH_URL` (default: 'http://localhost:9200/')
- `ELASTICSEARCH_INDEX` - (default 'eve')

### Facets setup

To add a facets support for specific resource, add `facets` into its `datasource`:

```python
DOMAIN = {
    'contacts': {
        'datasource':
            'facets': {
                'urgency': {'terms': {'field': 'urgency'}},
                'versioncreated': {'date_histogram': {'field': 'versioncreated', 'interval': 'hour'}}
            }
        }
    }
```

You will find more info about facets in [elasticsearch docs](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-facets.html).

### License

[GPLv3](http://www.gnu.org/licenses/gpl-3.0.txt)
