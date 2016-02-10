
import ast
import json
import arrow
import logging
import elasticsearch

from elasticsearch.helpers import bulk

from flask import request, current_app
from eve.utils import config
from eve.io.base import DataLayer
from uuid import uuid4


logger = logging.getLogger('elastic')


def parse_date(date_str):
    """Parse elastic datetime string."""
    if not date_str:
        return None

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


def generate_index_name(alias):
    random = str(uuid4()).split('-')[0]
    return '{}_{}'.format(alias, random)


class InvalidSearchString(Exception):
    '''Exception thrown when search string has invalid value'''
    pass


class InvalidIndexSettings(Exception):
    """Exception is thrown when put_settings is called without ELASTIC_SETTINGS"""
    pass


class ElasticJSONSerializer(elasticsearch.JSONSerializer):
    """Customize the JSON serializer used in Elastic."""
    pass


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
        return int(self.hits.get('hits', {}).get('total', '0'))

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


def get_es(url, **kwargs):
    """Create elasticsearch client instance.

    :param url: elasticsearch url
    """
    es = elasticsearch.Elasticsearch([url], **kwargs)
    return es


def get_indices(es):
    return elasticsearch.client.IndicesClient(es)


class Elastic(DataLayer):
    """ElasticSearch data layer."""

    serializers = {
        'integer': int,
        'datetime': parse_date,
    }

    def __init__(self, app, **kwargs):
        """Let user specify extra arguments for Elasticsearch"""
        self.kwargs = kwargs
        super(Elastic, self).__init__(app)

    def init_app(self, app):
        app.config.setdefault('ELASTICSEARCH_URL', 'http://localhost:9200/')
        app.config.setdefault('ELASTICSEARCH_INDEX', 'eve')
        app.config.setdefault('ELASTICSEARCH_INDEXES', {})
        app.config.setdefault('ELASTICSEARCH_FORCE_REFRESH', True)

        self.index = app.config['ELASTICSEARCH_INDEX']
        self.es = get_es(app.config['ELASTICSEARCH_URL'], **self.kwargs)

        indexes = list(app.config['ELASTICSEARCH_INDEXES'].values()) + [self.index]
        for index in indexes:
            if not self.es.indices.exists(index):
                self.create_index(index, app.config.get('ELASTICSEARCH_SETTINGS'))
                self.put_mapping(app, index)

    def get_datasource(self, resource):
        if hasattr(self, '_datasource'):
            return self._datasource(resource)
        return self.datasource(resource)

    def _get_mapping(self, schema):
        """Get mapping for given resource or item schema.

        :param schema: resource or dict/list type item schema
        """
        properties = {}
        for field, field_schema in schema.items():
            field_mapping = self._get_field_mapping(field_schema)
            if field_mapping:
                properties[field] = field_mapping
        return {'properties': properties}

    def _get_field_mapping(self, schema):
        """Get mapping for single field schema.

        :param schema: field schema
        """
        if 'mapping' in schema:
            return schema['mapping']
        elif schema['type'] == 'dict' and 'schema' in schema:
            return self._get_mapping(schema['schema'])
        elif schema['type'] == 'list' and 'schema' in schema.get('schema', {}):
            return self._get_mapping(schema['schema']['schema'])
        elif schema['type'] == 'datetime':
            return {'type': 'date'}
        elif schema['type'] == 'string' and schema.get('unique'):
            return {'type': 'string', 'index': 'not_analyzed'}

    def create_index(self, index=None, settings=None):
        """Create new index and ignore if it exists already."""
        if index is None:
            index = self.index
        try:
            alias = index
            index = generate_index_name(alias)

            args = {'index': index}
            if settings:
                args['body'] = settings

            self.es.indices.create(**args)
            self.es.indices.put_alias(index, alias)
            logger.info('created index alias=%s index=%s' % (alias, index))
        except elasticsearch.TransportError:  # index exists
            pass

    def put_mapping(self, app, index=None):
        """Put mapping for elasticsearch for current schema.

        It's not called automatically now, but rather left for user to call it whenever it makes sense.
        """

        for resource, resource_config in app.config['DOMAIN'].items():
            datasource = resource_config.get('datasource', {})

            if not is_elastic(datasource):
                continue

            if datasource.get('source', resource) != resource:  # only put mapping for core types
                continue

            properties = self._get_mapping(resource_config['schema'])
            properties['properties'].update({
                config.DATE_CREATED: self._get_field_mapping({'type': 'datetime'}),
                config.LAST_UPDATED: self._get_field_mapping({'type': 'datetime'}),
            })

            kwargs = {
                'index': index or self.index,
                'doc_type': resource,
                'body': properties,
                'ignore_conflicts': True,
            }

            try:
                self.es.indices.put_mapping(**kwargs)
            except elasticsearch.exceptions.RequestError:
                logger.warning('mapping error, updating settings resource=%s' % resource)
                self.put_settings(app, index)
                self.es.indices.put_mapping(**kwargs)

    def get_mapping(self, index, doc_type=None):
        """Get mapping for index.

        :param index: index name
        """
        mapping = self.es.indices.get_mapping(index=index, doc_type=doc_type)
        return next(iter(mapping.values()))

    def get_settings(self, index):
        """Get settings for index.

        :param index: index name
        """
        settings = self.es.indices.get_settings(index=index)
        return next(iter(settings.values()))

    def get_index_by_alias(self, alias):
        """Get index name for given alias.

        If there is no alias assume it's an index.

        :param alias: alias name
        """
        try:
            info = self.es.indices.get_alias(name=alias)
            return next(iter(info.keys()))
        except elasticsearch.exceptions.NotFoundError:
            return alias

    def find(self, resource, req, sub_resource_lookup):
        args = getattr(req, 'args', request.args if request else {}) or {}
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
                                                                      default_field=args.get('df', '_all'),
                                                                      default_operator=args.get('default_operator', 'OR'))

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
        filters.extend(args.get('filters') if 'filters' in args else [])
        set_filters(query, filters)

        if 'facets' in source_config:
            query['facets'] = source_config['facets']

        if 'aggregations' in source_config:
            query['aggs'] = source_config['aggregations']

        args = self._es_args(resource)
        try:
            hits = self.es.search(body=query, **args)
        except elasticsearch.exceptions.RequestError as e:
            if e.status_code == 400 and "No mapping found for" in e.error:
                hits = {}
            elif e.status_code == 400 and 'SearchParseException' in e.error:
                raise InvalidSearchString
            else:
                raise
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
            res = self.es.index(body=doc, id=doc.get('_id'), **kwargs)
            ids.append(res.get('_id', doc.get('_id')))
        self._refresh_resource_index(resource)
        return ids

    def bulk_insert(self, resource, docs, **kwargs):
        kwargs.update(self._es_args(resource))
        res = bulk(self.es, docs, stats_only=False, **kwargs)
        self._refresh_resource_index(resource)
        return res

    def update(self, resource, id_, updates):
        args = self._es_args(resource, refresh=True)
        return self.es.update(id=id_, body={'doc': updates}, **args)

    def replace(self, resource, id_, document):
        args = self._es_args(resource, refresh=True)
        return self.es.index(body=document, id=id_, **args)

    def remove(self, resource, lookup=None):
        args = self._es_args(resource)
        if lookup:
            if lookup.get('_id'):
                try:
                    return self.es.delete(id=lookup.get('_id'), refresh=True, **args)
                except elasticsearch.NotFoundError:
                    return
            else:
                return self.es.delete_by_query(body=lookup, **args)
        else:
            query = {'query': {'match_all': {}}}
            return self.es.delete_by_query(body=query, **args)

    def is_empty(self, resource):
        args = self._es_args(resource)
        res = self.es.count(body={'query': {'match_all': {}}}, **args)
        return res.get('count', 0) == 0

    def put_settings(self, app=None, index=None, settings=None):
        """Modify index settings"""
        if not settings and app:
            settings = app.config.get('ELASTICSEARCH_SETTINGS')

        if not settings:
            raise InvalidIndexSettings

        if not index:
            index = self.index

        index_exists = get_indices(self.es).exists(index)

        if index_exists:
            self.es.indices.close(index=index)
            self.es.indices.put_settings(index=index, body=settings)
            self.es.indices.open(index=index)
        else:
            self.create_index(index, settings)

    def _parse_hits(self, hits, resource):
        """Parse hits response into documents."""
        datasource = self.get_datasource(resource)
        schema = {}
        schema.update(config.DOMAIN[datasource[0]].get('schema', {}))
        schema.update(config.DOMAIN[resource].get('schema', {}))
        dates = get_dates(schema)
        docs = []
        for hit in hits.get('hits', {}).get('hits', []):
            docs.append(format_doc(hit, schema, dates))
        return ElasticCursor(hits, docs)

    def _es_args(self, resource, refresh=None):
        """Get index and doctype args."""
        datasource = self.get_datasource(resource)
        args = {
            'index': self._resource_index(datasource[0]),
            'doc_type': datasource[0],
        }
        if refresh:
            args['refresh'] = refresh
        return args

    def _fields(self, resource):
        """Get projection fields for given resource."""
        datasource = self.get_datasource(resource)
        keys = datasource[2].keys()
        return ','.join(keys) + ','.join([config.LAST_UPDATED, config.DATE_CREATED])

    def _default_sort(self, resource):
        datasource = self.get_datasource(resource)
        return datasource[3]

    def _resource_index(self, resource):
        """Get index for given resource.

        by default it will be `self.index`, but it can be overriden via app.config

        :param resource: resource name
        """
        return current_app.config['ELASTICSEARCH_INDEXES'].get(resource, self.index)

    def _refresh_resource_index(self, resource):
        """Refresh index for given resource.

        :param resource: resource name
        """
        if current_app.config.get('ELASTICSEARCH_FORCE_REFRESH'):
            datasource = self.get_datasource(resource)
            get_indices(self.es).refresh(self._resource_index(datasource[0]))


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


def _build_query_string(q, default_field=None, default_operator='AND'):
    """
    Builds "query_string" object from 'q'.

    :param: q of type String
    :param: default_field
    :return: dictionary object.
    """
    def _is_phrase_search(query_string):
        clean_query = query_string.strip()
        return clean_query and clean_query.startswith('"') and clean_query.endswith('"')

    def _get_phrase(query_string):
        return query_string.strip().strip('"')

    if _is_phrase_search(q):
        query = {'match_phrase': {'_all': _get_phrase(q)}}
    else:
        query = {'query_string': {'query': q, 'default_operator': default_operator}}
        query['query_string'].update({'lenient': False} if default_field else {'default_field': default_field})

    return query
