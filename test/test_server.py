#!/usr/bin/env python3

from http.server import HTTPServer, BaseHTTPRequestHandler
from random import randint
from socketserver import ThreadingMixIn
from time import sleep


class ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
    pass


class RequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == '/slow':
            sleep(3)
            self._send_response((1, 2, 3))
        elif self.path == '/empty':
            self._send_response([])
        elif self.path == '/incorrect':
            self._send_response(('foo', 'bar'))
        elif self.path.startswith('/amount'):
            count = int(self.path.rsplit('/', 1)[1])
            self._send_response(randint(-1000, 1000) for _ in range(count))
        else:
            self._send_response([], 404)

    def _send_response(self, numbers, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'plain/text')
        self.end_headers()
        self.wfile.write(', '.join(map(str, numbers)).encode())


if __name__ == '__main__':
    server = ThreadingSimpleServer(('', 8889), RequestHandler)
    server.serve_forever()
