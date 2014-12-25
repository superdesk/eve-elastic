
import ast
import json
import arrow
import elasticsearch

from bson import ObjectId
from flask import request
from eve.utils import config
from eve.io.base import DataLayer

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


def parse_date(date_str):
    """Parse elastic datetime string."""
    try:
        date = arrow.get(date_str)
    except TypeError:
        date = arrow.get(date_str[0])
    return date.datetime


def get_dates(schema):
    """Return list of datetime fields for given schema."""
    dates = [config.LAST_UPDATED, config.DATE_CREATED]
    for field, field_schema in schema.items():
        if field_schema['type'] == 'datetime':
            dates.append(field)
    return dates


def format_doc(hit, schema, dates):
    """Format given doc to match given schema."""
    doc = hit.get('_source', {})
    doc.setdefault(config.ID_FIELD, hit.get('_id'))
    doc.setdefault('_type', hit.get('_type'))

    for key in dates:
        if key in doc:
            doc[key] = parse_date(doc[key])

    return doc


def noop():
    pass


def is_elastic(datasource):
    """Detect if given resource uses elastic."""
    return datasource.get('backend') == 'elastic' or datasource.get('search_backend') == 'elastic'


class ElasticJSONSerializer(elasticsearch.JSONSerializer):
    """Customize the JSON serializer used in Elastic"""
    def default(self, value):
        """Convert mongo.ObjectId."""
        if isinstance(value, ObjectId):
            return str(value)
        return super().default(value)


class ElasticCursor(object):
    """Search results cursor."""

    no_hits = {'hits': {'total': 0, 'hits': []}}

    def __init__(self, hits=None, docs=None):
        """Parse hits into docs."""
        self.hits = hits if hits else self.no_hits
        self.docs = docs if docs else []

    def __getitem__(self, key):
        return self.docs[key]

    def first(self):
        """Get first doc."""
        return self.docs[0] if self.docs else None

    def count(self, **kwargs):
        """Get hits count."""
        return int(self.hits['hits']['total'])

    def extra(self, response):
        """Add extra info to response."""
        if 'facets' in self.hits:
            response['_facets'] = self.hits['facets']
        if 'aggregations' in self.hits:
            response['_aggregations'] = self.hits['aggregations']


def set_filters(query, base_filters):
    """Put together all filters we have and set them as 'and' filter
    within filtered query.

    :param query: elastic query being constructed
    :param base_filters: all filters set outside of query (eg. resource config, sub_resource_lookup)
    """
    filters = [f for f in base_filters if f is not None]
    query_filter = query['query']['filtered'].get('filter', None)
    if query_filter is not None:
        if 'and' in query_filter:
            filters.extend(query_filter['and'])
        else:
            filters.append(query_filter)
    if filters:
        query['query']['filtered']['filter'] = {'and': filters}


def set_sort(query, sort):
    query['sort'] = []
    for (key, sortdir) in sort:
        sort_dict = dict([(key, 'asc' if sortdir > 0 else 'desc')])
        query['sort'].append(sort_dict)


def get_es(url):
    o = urlparse(url)
    es = elasticsearch.Elasticsearch(hosts=[{'host': o.hostname, 'port': o.port}])
    es.transport.serializer = ElasticJSONSerializer()
    return es


def get_indices(es):
    return elasticsearch.client.IndicesClient(es)


class Elastic(DataLayer):
    """ElasticSearch data layer."""

    serializers = {
        'integer': int,
        'datetime': parse_date,
        'objectid': ObjectId,
    }

    def init_app(self, app):
        app.config.setdefault('ELASTICSEARCH_URL', 'http://localhost:9200/')
        app.config.setdefault('ELASTICSEARCH_INDEX', 'eve')

        self.index = app.config['ELASTICSEARCH_INDEX']
        self.es = get_es(app.config['ELASTICSEARCH_URL'])

        self.create_index(self.index)
        self.put_mapping(app)

    def _get_field_mapping(self, schema):
        """Get mapping for given field schema."""
        if 'mapping' in schema:
            return schema['mapping']
        elif schema['type'] == 'datetime':
            return {'type': 'date'}
        elif schema['type'] == 'string' and schema.get('unique'):
            return {'type': 'string', 'index': 'not_analyzed'}

    def create_index(self, index=None):
        if index is None:
            index = self.index
        try:
            get_indices(self.es).create(self.index)
        except elasticsearch.TransportError:
            pass

    def put_mapping(self, app):
        """Put mapping for elasticsearch for current schema.

        It's not called automatically now, but rather left for user to call it whenever it makes sense.
        """

        indices = get_indices(self.es)

        for resource, resource_config in app.config['DOMAIN'].items():
            datasource = resource_config.get('datasource', {})

            if not is_elastic(datasource):
                continue

            if datasource.get('source', resource) != resource:  # only put mapping for core types
                continue

            properties = {}
            properties[config.DATE_CREATED] = self._get_field_mapping({'type': 'datetime'})
            properties[config.LAST_UPDATED] = self._get_field_mapping({'type': 'datetime'})

            for field, schema in resource_config['schema'].items():
                field_mapping = self._get_field_mapping(schema)
                if field_mapping:
                    properties[field] = field_mapping

            mapping = {'properties': properties}
            indices.put_mapping(index=self.index, doc_type=resource, body=mapping, ignore_conflicts=True)

    def find(self, resource, req, sub_resource_lookup):
        args = getattr(req, 'args', request.args if request else {})
        source_config = config.SOURCES[resource]

        if args.get('source'):
            query = json.loads(args.get('source'))

            if 'filtered' not in query.get('query', {}):
                _query = query.get('query')
                query['query'] = {'filtered': {}}
                if _query:
                    query['query']['filtered']['query'] = _query
        else:
            query = {'query': {'filtered': {}}}

        if args.get('q', None):
            query['query']['filtered']['query'] = _build_query_string(args.get('q'),
                                                                      default_field=args.get('df', '_all'))

        if 'sort' not in query:
            if req.sort:
                sort = ast.literal_eval(req.sort)
                set_sort(query, sort)
            elif self._default_sort(resource) and 'query' not in query['query']['filtered']:
                set_sort(query, self._default_sort(resource))

        if req.max_results:
            query.setdefault('size', req.max_results)

        if req.page > 1:
            query.setdefault('from', (req.page - 1) * req.max_results)

        filters = []
        filters.append(source_config.get('elastic_filter'))
        filters.append(source_config.get('elastic_filter_callback', noop)())
        filters.append({'term': sub_resource_lookup} if sub_resource_lookup else None)
        filters.append(json.loads(args.get('filter')) if 'filter' in args else None)
        set_filters(query, filters)

        if 'facets' in source_config:
            query['facets'] = source_config['facets']

        if 'aggregations' in source_config:
            query['aggs'] = source_config['aggregations']

        args = self._es_args(resource)
        hits = self.es.search(body=query, **args)
        return self._parse_hits(hits, resource)

    def find_one(self, resource, req, **lookup):

        def is_found(hit):
            if 'exists' in hit:
                hit['found'] = hit['exists']
            return hit.get('found', False)

        args = self._es_args(resource)

        if config.ID_FIELD in lookup:
            try:
                hit = self.es.get(id=lookup[config.ID_FIELD], **args)
            except elasticsearch.NotFoundError:
                return

            if not is_found(hit):
                return

            docs = self._parse_hits({'hits': {'hits': [hit]}}, resource)
            return docs.first()
        else:
            query = {
                'query': {
                    'term': lookup
                }
            }

            try:
                args['size'] = 1
                hits = self.es.search(body=query, **args)
                docs = self._parse_hits(hits, resource)
                return docs.first()
            except elasticsearch.NotFoundError:
                return

    def find_one_raw(self, resource, _id):
        args = self._es_args(resource)
        hit = self.es.get(id=_id, **args)
        return self._parse_hits({'hits': {'hits': [hit]}}, resource).first()

    def find_list_of_ids(self, resource, ids, client_projection=None):
        args = self._es_args(resource)
        return self._parse_hits(self.es.multi_get(ids, **args), resource)

    def insert(self, resource, doc_or_docs, **kwargs):
        ids = []
        kwargs.update(self._es_args(resource))
        for doc in doc_or_docs:
            doc.update(self.es.index(body=doc, id=doc.get('_id'), **kwargs))
            ids.append(doc['_id'])
        get_indices(self.es).refresh(self.index)
        return ids

    def update(self, resource, id_, updates):
        args = self._es_args(resource, refresh=True)
        return self.es.update(id=id_, body={'doc': updates}, **args)

    def replace(self, resource, id_, document):
        args = self._es_args(resource, refresh=True)
        return self.es.index(body=document, id=id_, **args)

    def remove(self, resource, lookup=None):
        args = self._es_args(resource)
        if lookup:
            try:
                return self.es.delete(id=lookup.get('_id'), refresh=True, **args)
            except elasticsearch.NotFoundError:
                return
        else:
            query = {'query': {'match_all': {}}}
            return self.es.delete_by_query(body=query, **args)

    def is_empty(self, resource):
        args = self._es_args(resource)
        res = self.es.count(body={'query': {'match_all': {}}}, **args)
        return res.get('count', 0) == 0

    def get_mapping(self, index, doc_type=None):
        return get_indices(self.es).get_mapping(index=index, doc_type=doc_type)

    def _parse_hits(self, hits, resource):
        """Parse hits response into documents."""
        datasource = self._datasource(resource)
        schema = config.DOMAIN[datasource[0]]['schema']
        dates = get_dates(schema)
        docs = []
        for hit in hits.get('hits', {}).get('hits', []):
            docs.append(format_doc(hit, schema, dates))
        return ElasticCursor(hits, docs)

    def _es_args(self, resource, refresh=None):
        """Get index and doctype args."""
        datasource = self._datasource(resource)
        args = {
            'index': self.index,
            'doc_type': datasource[0],
            }
        if refresh:
            args['refresh'] = refresh
        return args

    def _fields(self, resource):
        """Get projection fields for given resource."""
        datasource = self._datasource(resource)
        keys = datasource[2].keys()
        return ','.join(keys) + ','.join([config.LAST_UPDATED, config.DATE_CREATED])

    def _default_sort(self, resource):
        datasource = self._datasource(resource)
        return datasource[3]


def build_elastic_query(doc):
    """
    Builds a query which follows ElasticSearch syntax from doc.
    1. Converts {"q":"cricket"} to the below elastic query
    {
        "query": {
            "filtered": {
                "query": {
                    "query_string": {
                        "query": "cricket",
                        "lenient": false,
                        "default_operator": "AND"
                    }
                }
            }
        }
    }

    2. Converts a faceted query
    {"q":"cricket", "type":['text'], "source": "AAP"}
    to the below elastic query
    {
        "query": {
            "filtered": {
                "filter": {
                    "and": [
                        {"terms": {"type": ["text"]}},
                        {"term": {"source": "AAP"}}
                    ]
                },
                "query": {
                    "query_string": {
                        "query": "cricket",
                        "lenient": false,
                        "default_operator": "AND"
                    }
                }
            }
        }
    }

    :param doc: A document object which is inline with the syntax specified in the examples.
                It's the developer responsibility to pass right object.
    :returns ElasticSearch query
    """

    elastic_query, filters = {"query": {"filtered": {}}}, []

    for key in doc.keys():
        if key == 'q':
            elastic_query['query']['filtered']['query'] = _build_query_string(doc['q'])
        else:
            _value = doc[key]
            filters.append({"terms": {key: _value}} if isinstance(_value, list) else {"term": {key: _value}})

    set_filters(elastic_query, filters)
    return elastic_query


def _build_query_string(q, default_field=None):
    """
    Builds "query_string" object from 'q'.

    :param: q of type String or Unicode
    :param: default_field
    :return: dictionary object.
    """

    if not (isinstance(q, str) or isinstance(q, unicode)):
        raise TypeError("Invalid type %s for 'q'" % type(q))

    query_string = {'query_string': {'query': q, 'default_operator': 'AND'}}
    query_string['query_string'].update({'lenient': False} if default_field else {'default_field': default_field})

    return query_string
