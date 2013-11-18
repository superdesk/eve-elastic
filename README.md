## eve-elastic

Elasticsearch data layer for [eve REST framework](http://python-eve.org).

### Usage:

```python

import eve
from eve_elastic import Elastic

app = Eve(data=Elastic)

```

### TODO v0.1:

- [x] facets support
- [ ] auto put mapping
- [ ] tests
