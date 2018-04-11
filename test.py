from dataman.client import DatamanClient
from dataman.query import QueryArgs
import tornado.web
import tornado.ioloop
import tornado.gen
import random
import json
from functools import partial


def write_to_html(text):
    text = json.dumps(json.loads(text),indent=4)
    text = text.replace(" ","&nbsp;")
    text = text.replace("\n","<br />")
    return text + "<br />"

class ReadHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        resp = yield self.application.client.filter(body=QueryArgs('test1','user',data_filter={},sort=['username']).serialize())
        self.write(write_to_html(resp.buffer.read()))

class AggregateHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        resp = yield self.application.client.aggregate(body=QueryArgs('test1','user',data_filter={},aggregation_fields={'username' : ['count']}).serialize())
        self.write(write_to_html(resp.buffer.read()))

class StreamingReadHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        def write_callback(c):
            self.write(write_to_html(c))

        resp = yield self.application.client.filter_stream(streaming_callback=write_callback, body=QueryArgs('test1','user',data_filter={},sort=['username']).serialize())
        self.write("<br /><br />" + resp.reason)

class WriteHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        resp = yield self.application.client.set(body=QueryArgs('test1','user',record=
            {'username' : str(random.random()),
             'firstname' : str(random.random()),
             'lastname' : str(random.random())}).serialize())
        self.write(write_to_html(resp.buffer.read()))

class DatamanProxyServer(tornado.web.Application):

    def __init__(self, dataman_client, *args, **kwargs):
        self.client = dataman_client
        super(DatamanProxyServer, self).__init__(*args, **kwargs)

if __name__ == '__main__':
    app = DatamanProxyServer(DatamanClient('10.10.100.8',8080), [
        (r"/", ReadHandler),
        (r"/stream", StreamingReadHandler),
        (r"/set", WriteHandler),
        (r"/agg", AggregateHandler),
    ])
    app.listen(8089)
    tornado.ioloop.IOLoop.current().start()

