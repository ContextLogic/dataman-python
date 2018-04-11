import tornado.ioloop
import tornado.gen
import pycurl
import ujson
import urllib
import copy

from dataman.http import HttpClientSingleton
from dataman.query import OPERATIONS

from functools import partial

class WrappedBuffer(object):

    def __init__(self):
        self.buf = []

class DatamanClient(object):

    DATA_PATH = 'data/raw/{operation}'

    def __init__(self, host, port, use_curl=True, ioloop=None, secure=False,
            version='v1'):
        self.host = host
        self.port = port
        self.use_curl = use_curl
        self.secure = secure
        self.version = version

        if ioloop is None:
            ioloop = tornado.ioloop.IOLoop.current()
        self.ioloop = ioloop

        for operation in OPERATIONS:
            path = self._path(self.DATA_PATH.format(operation=operation.op))
            if operation.streaming:
                func = partial(self._make_streaming_request,path,"POST")
            else:
                func = partial(self._make_request,path,"POST")
            setattr(self, operation.op, func)

    @classmethod
    def _prepare_request_args(cls, method, **params):
        body = params.pop('body', None)
        auth = params.pop('auth', None)

        params['method'] = method
        # These are the default tornado timeout values.
        params['request_timeout'] = params.get('request_timeout', 20.0)
        params['connect_timeout'] = params.get('connect_timeout', 20.0)

        if 'use_curl' in params:
            params['prepare_curl_callback'] = lambda c: c.setopt(pycurl.TCP_NODELAY, 1)

        if 'headers' not in params:
            params['headers'] = {}

        if method in ('PUT', 'POST', 'PATCH'):
            params['body'] = body or ''

        if auth:
            params['headers']['Authorization'] = auth

        return params

    def _path(self, path):
        return '{scheme}://{host}:{port}/{version}/{path}'.format(
                scheme = 'http' if not self.secure else 'https',
                host   = self.host,
                port   = self.port,
                version = self.version,
                path   = path,
            )

    @tornado.gen.coroutine
    def _make_request(self, path, method, **kwargs):
        async = kwargs.pop('async',True)
        kwargs = self._prepare_request_args(method, **kwargs)

        client = HttpClientSingleton.instance().async_curl if async else \
            HttpClientSingleton.instance().sync_curl
        if not self.use_curl:
            client = HTTPClientSingleton.isntance().async if async else \
                HttpClientSingleton.instance().sync
        resp = yield client.fetch(path, **kwargs)
        raise tornado.gen.Return(resp)

    # NOTE: The streaming_callback will be passed full chunks, and does not
    # have to deal with piecing them together.
    @tornado.gen.coroutine
    def _make_streaming_request(self, path, method, streaming_callback, **kwargs):
        if not callable(streaming_callback):
            raise ValueError("Must pass in a callable streaming_callback")
        kwargs['streaming_callback'] = partial(
            self._handle_chunked_streaming_response,
            WrappedBuffer(),
            streaming_callback
        )
        resp = yield self._make_request(path, method, **kwargs)
        raise tornado.gen.Return(resp)

    # Convenience method for dealing with chunked-encoded streams
    @classmethod
    def _handle_chunked_streaming_response(cls, buf, callback, chunk):
        if "\n" not in chunk:
            buf.buf.append(chunk)
            return

        lines = chunk.split("\n")
        lines[0] = "".join(buf.buf) + lines[0]
        buf.buf = []

        if not chunk.endswith("\n"):
            buf.buf = [lines.pop(len(lines) - 1)]

        for line in lines:
            if line == "":
                continue
            # We schedule the callback on the ioloop since for certain
            # execution models (like greenlets) there's no guarantee
            # that we'll switch back here. For only one line, it's fine
            # but for multiple lines, we need to add the callbacks to the ioloop
            # so they all eventually get called in order.
            tornado.ioloop.IOLoop.current().add_callback(
                partial(callback,line))

