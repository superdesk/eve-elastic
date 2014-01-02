
import ast
import arrow
import pyelasticsearch.exceptions as es_exceptions
from pyelasticsearch import ElasticSearch
from flask import request, json
from eve.io.base import DataLayer
from eve.utils import config


def parse_date(date_str):
    """Parse elastic datetime string."""
    return arrow.get(date_str).datetime


def convert_dates(doc, dates):
    """Convert dates in doc into datetime objects."""
    for date_field in dates:
        if date_field in doc:
            doc[date_field] = parse_date(doc[date_field])


class ElasticCursor(object):
    """Search results cursor."""

    no_hits = {'hits': {'total': 0, 'hits': []}}

    def __init__(self, hits=None, dates=None):
        """Parse hits into docs."""
        self.hits = hits if hits else self.no_hits
        self.docs = []

        for hit in self.hits['hits']['hits']:
            doc = hit.get('fields', hit.get('_source', {}))
            doc[config.ID_FIELD] = hit.get('_id')
            convert_dates(doc, dates)
            self.docs.append(doc)

    def __getitem__(self, key):
        return self.docs[key]

    def first(self):
        """Get first doc."""
        return self.docs[0] if self.docs else None

    def count(self):
        """Get hits count."""
        return int(self.hits['hits']['total'])

    def extra(self, response):
        """Add extra info to response."""
        if 'facets' in self.hits:
            response['_facets'] = self.hits['facets']


class Elastic(DataLayer):
    """ElasticSearch data layer."""

    serializers = {
        'integer': int,
        'datetime': parse_date
    }

    def init_app(self, app):
        app.config.setdefault('ELASTICSEARCH_URL', 'http://localhost:9200/')
        app.config.setdefault('ELASTICSEARCH_INDEX', 'eve')
        self.es = ElasticSearch(app.config['ELASTICSEARCH_URL'])
        self.index = app.config['ELASTICSEARCH_INDEX']

    def _get_field_mapping(self, schema):
        """Get mapping for given field schema."""
        if schema['type'] == 'datetime':
            return {'type': 'date'}
        elif schema['type'] == 'string' and schema.get('unique'):
            return {'type': 'string', 'index': 'not_analyzed'}
        elif schema['type'] == 'string':
            return {'type': 'string'}

    def put_mapping(self, app):
        """Put mapping for elasticsearch for current schema.

        It's not called automatically now, but rather left for user to call it whenever it makes sense.
        """
        for resource, resource_config in app.config['DOMAIN'].items():
            properties = {}
            properties[config.DATE_CREATED] = self._get_field_mapping({'type': 'datetime'})
            properties[config.LAST_UPDATED] = self._get_field_mapping({'type': 'datetime'})

            for field, schema in resource_config['schema'].items():
                field_mapping = self._get_field_mapping(schema)
                if field_mapping:
                    properties[field] = field_mapping

            datasource = (resource, ) #  TODO: config.SOURCES not available yet (self._datasource_ex(resource))
            mapping = {}
            mapping[datasource[0]] = {'properties': properties}
            self.es.put_mapping(self.index, datasource[0], mapping)

    def find(self, resource, req, sub_resource_lookup):
        """
        TODO: implement sub_resource_lookup
        """
        query = {
            'query': {
                'query_string': {
                    'query': request.args.get('q', '*'),
                    'default_field': request.args.get('df', '_all'),
                    'default_operator': 'AND'
                }
            }
        }

        if not req.sort and self._default_sort(resource):
            req.sort = self._default_sort(resource)

        # skip sorting when there is a query to use score
        if req.sort and 'q' not in request.args:
            query['sort'] = []
            sort = ast.literal_eval(req.sort)
            for (key, sortdir) in sort:
                sort_dict = dict([(key, 'asc' if sortdir > 0 else 'desc')])
                query['sort'].append(sort_dict)

        if req.where:
            where = json.loads(req.where)
            if where:
                query['filter'] = {
                    'term': where
                }

        if req.max_results:
            query['size'] = req.max_results

        if req.page > 1:
            query['from'] = (req.page - 1) * req.max_results

        source_config = config.SOURCES[resource]
        if 'facets' in source_config:
            query['facets'] = source_config['facets']

        try:
            args = self._es_args(resource)
            args['es_fiels'] = self._fields(resource)
            return self._parse_hits(self.es.search(query, **args), resource)
        except es_exceptions.ElasticHttpError:
            return ElasticCursor()

    def find_one(self, resource, **lookup):
        args = self._es_args(resource)
        args['es_fields'] = self._fields(resource)

        if config.ID_FIELD in lookup:
            try:
                hit = self.es.get(id=lookup[config.ID_FIELD], **args)
            except es_exceptions.ElasticHttpNotFoundError:
                return

            if not hit['exists']:
                return

            doc = hit.get('fields', hit.get('_source', {}))
            doc['_id'] = hit.get('_id')
            convert_dates(doc, self._dates(resource))
            return doc
        else:
            query = {
                'query': {
                    'constant_score': {
                        'filter': {
                            'term': lookup
                        }
                    }
                }
            }

            try:
                args['size'] = 1
                docs = self._parse_hits(self.es.search(query, **args), resource)
                return docs.first()
            except es_exceptions.ElasticHttpNotFoundError:
                return None

    def find_list_of_ids(self, resource, ids, client_projection=None):
        args = self._es_args(resource)
        args['es_fields'] = self._fields(resource)
        return self._parse_hits(self.es.multi_get(ids, **args), resource)

    def insert(self, resource, doc_or_docs, **kwargs):
        ids = []
        kwargs.update(self._es_args(resource))
        for doc in doc_or_docs:
            doc.update(self.es.index(doc=doc, id=doc.get('_id'), **kwargs))
            ids.append(doc['_id'])
        self.es.refresh(self.index)
        return ids

    def update(self, resource, id_, updates):
        args = self._es_args(resource, refresh=True)
        return self.es.update(id=id_, doc=updates, **args)

    def replace(self, resource, id_, document):
        args = self._es_args(resource, refresh=True)
        args['overwrite_existing'] = True
        return self.es.index(document=document, id=id_, **args)

    def remove(self, resource, id_=None):
        args = self._es_args(resource, refresh=True)
        if id_:
            return self.es.delete(id=id_, **args)
        else:
            try:
                return self.es.delete_all(**args)
            except es_exceptions.ElasticHttpNotFoundError:
                return

    def _parse_hits(self, hits, resource):
        """Parse hits response into documents."""
        return ElasticCursor(hits, self._dates(resource))

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
        return ','.join(keys)

    def _default_sort(self, resource):
        datasource = self._datasource(resource)
        return datasource[3]

    def _dates(self, resource):
        dates = [config.LAST_UPDATED, config.DATE_CREATED]
        datasource = self._datasource(resource)
        schema = config.DOMAIN[datasource[0]]['schema']
        for field, field_schema in schema.items():
            if field_schema['type'] == 'datetime':
                dates.append(field)
        return dates
