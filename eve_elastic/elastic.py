
import ast
import arrow
import pyelasticsearch.exceptions as es_exceptions
from bson import ObjectId
from pyelasticsearch import ElasticSearch
from flask import request, json
from eve.io.base import DataLayer
from eve.utils import config
from pyelasticsearch.exceptions import IndexAlreadyExistsError


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

    for key in dates:
        if key in doc:
            doc[key] = parse_date(doc[key])

    return doc


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
        'datetime': parse_date,
        'objectid': ObjectId,
    }

    def init_app(self, app):
        app.config.setdefault('ELASTICSEARCH_URL', 'http://localhost:9200/')
        app.config.setdefault('ELASTICSEARCH_INDEX', 'eve')
        self.es = ElasticSearch(app.config['ELASTICSEARCH_URL'])
        self.index = app.config['ELASTICSEARCH_INDEX']

        try:
            self.es.create_index(self.index)
        except IndexAlreadyExistsError:
            pass

        self.put_mapping(app)

    def _get_field_mapping(self, schema):
        """Get mapping for given field schema."""
        if schema['type'] == 'datetime':
            return {'type': 'date'}
        elif schema['type'] == 'string' and schema.get('unique'):
            return {'type': 'string', 'index': 'not_analyzed'}

    def put_mapping(self, app):
        """Put mapping for elasticsearch for current schema.

        It's not called automatically now, but rather left for user to call it whenever it makes sense.
        """
        for resource, resource_config in app.config['DOMAIN'].items():
            datasource = resource_config.get('datasource', {})

            if datasource.get('backend') != 'elastic':  # only put mapping for elastic sources
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
            self.es.put_mapping(self.index, resource, mapping, es_ignore_conflicts=True)

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

        if request.args.get('filter'):
            # if there is a filter param, use it as is
            query_filter = json.loads(request.args.get('filter'))
            query['filter'] = query_filter
        elif req.where:
            # or use where as term filter
            where = json.loads(req.where)
            if where:
                query['filter'] = {'term': where}

        if req.max_results:
            query['size'] = req.max_results

        if req.page > 1:
            query['from'] = (req.page - 1) * req.max_results

        if request.args.get('source'):
            query = json.loads(request.args.get('source'))

        source_config = config.SOURCES[resource]
        if 'facets' in source_config:
            query['facets'] = source_config['facets']

        try:
            args = self._es_args(resource)
            return self._parse_hits(self.es.search(query, **args), resource)
        except es_exceptions.ElasticHttpError:
            return ElasticCursor()

    def find_one(self, resource, req, **lookup):

        def is_found(hit):
            if 'exists' in hit:
                hit['found'] = hit['exists']
            return hit['found']

        args = self._es_args(resource)

        if config.ID_FIELD in lookup:
            try:
                hit = self.es.get(id=lookup[config.ID_FIELD], **args)
            except es_exceptions.ElasticHttpNotFoundError:
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
                hits = self.es.search(query, **args)
                docs = self._parse_hits(hits, resource)
                return docs.first()
            except es_exceptions.ElasticHttpNotFoundError:
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
            doc.update(self.es.index(doc=self._format_doc(doc), id=doc.get('_id'), **kwargs))
            ids.append(doc['_id'])
        self.es.refresh(self.index)
        return ids

    def update(self, resource, id_, updates):
        args = self._es_args(resource, refresh=True)
        return self.es.update(id=id_, doc=self._format_doc(updates), **args)

    def replace(self, resource, id_, document):
        args = self._es_args(resource, refresh=True)
        args['overwrite_existing'] = True
        return self.es.index(document=document, id=id_, **args)

    def remove(self, resource, lookup=None):
        args = self._es_args(resource)
        try:
            if lookup:
                return self.es.delete(id=lookup.get('_id'), refresh=True, **args)
            else:
                query = {'query': {'match_all': {}}}
                return self.es.delete_by_query(query=query, **args)
        except es_exceptions.ElasticHttpNotFoundError:
            return

    def is_empty(self, resource):
        return self.es.count(resource, {}) == 0

    def _parse_hits(self, hits, resource):
        """Parse hits response into documents."""
        datasource = self._datasource(resource)
        schema = config.DOMAIN[datasource[0]]['schema']
        dates = get_dates(schema)
        docs = []
        for hit in hits['hits']['hits']:
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

    def _format_doc(self, doc):
        """Make document ready for storage."""
        for k, v in doc.items():
            if isinstance(v, ObjectId):
                doc[k] = str(v)
        return doc
    