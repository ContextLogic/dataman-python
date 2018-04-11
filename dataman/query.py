import ujson

class QueryType(object):

    def __init__(self, op, streaming=False):
        self.op = op
        self.streaming = streaming

class QueryArgs(object):

    def __init__(self, db, collection, shard_instance=None,
            fields=None, sort=None, sort_reverse=None,
            limit=None, offset=None, pkey=None, record=None,
            record_op=None, data_filter=None, join=None,
            aggregation_fields=None,ser=ujson.dumps):
        self.db = db
        self.collection = collection
        self.shard_instance = shard_instance
        self.fields = fields
        self.sort = sort
        self.sort_reverse = sort_reverse
        self.limit = limit
        self.offset = offset
        self.pkey = pkey
        self.record = record
        self.record_op = record_op
        self.filter = data_filter
        self.join = join
        self.aggregation_fields = aggregation_fields

        self.ser = ser

    def to_dict(self):
        return {
            "db" : self.db,
            "collection" : self.collection,
            'shard_instance' : self.shard_instance,
            "fields" : self.fields,
            "sort" : self.sort,
            "sort_reverse" : self.sort_reverse,
            "limit" : self.limit,
            "offset" : self.offset,
            "pkey" : self.pkey,
            "record" : self.record,
            "record_op" : self.record_op,
            "filter" : self.filter,
            "join" : self.join,
            "aggregation_fields" : self.aggregation_fields,
        }

    def serialize(self):
        return self.ser(self.to_dict())

OPERATIONS = [
    QueryType('get'),
    QueryType('set'),
    QueryType('insert'),
    QueryType('update'),
    QueryType('delete'),
    QueryType('filter'),
    QueryType('aggregate'),
    QueryType('filter_stream',streaming=True),
]

