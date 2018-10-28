import json
from http import HTTPStatus
from http.server import HTTPServer, BaseHTTPRequestHandler
from shutil import copyfileobj
from typing import Callable, Tuple, Optional, Union, Dict
from urllib.parse import parse_qs, urlparse


class CustomServer(HTTPServer):

    def __init__(self, *args, on_new_job: Callable, on_get_job_status: Callable, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_new_job = on_new_job
        self._on_get_job_status = on_get_job_status

    def finish_request(self, request, client_address):
        self.RequestHandlerClass(request, client_address, self, on_new_job=self._on_new_job,
                                 on_get_job_status=self._on_get_job_status)


class RequestHandler(BaseHTTPRequestHandler):

    def __init__(self, *args, on_new_job: Callable, on_get_job_status: Callable, **kwargs):
        self._on_new_job = on_new_job
        self._on_get_job_status = on_get_job_status
        super().__init__(*args, **kwargs)

    def do_GET(self):
        params = self._parse_params(self.path)
        status, payload = self._handle_request(params)
        self._send_response(status, payload)

    def _parse_params(self, path):
        return dict((key, value[0]) for key, value in parse_qs(urlparse(path).query).items())

    def _handle_request(self, params: Dict[str, str]) -> Tuple[HTTPStatus, Optional[Union[dict, str]]]:
        if 'concurrency' in params and 'sort' in params:
            job_id = self._on_new_job(params['concurrency'], params['sort'])
            return HTTPStatus.OK, dict(jobid=job_id)
        if 'get' in params:
            status = self._on_get_job_status(params['get'])
            if not status:
                return HTTPStatus.NOT_FOUND, dict(state='eexist', data=None)
            if status.has_file_path():
                return HTTPStatus.OK, status.data
            return HTTPStatus.OK, dict(state=status.state, data=status.data)
        return HTTPStatus.BAD_REQUEST, None

    def _send_response(self, status: HTTPStatus, payload: Union[dict, str]=None):
        self.send_response(status.value)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        if payload is not None:
            if isinstance(payload, str):
                with open(payload, 'rb') as file:
                    copyfileobj(file, self.wfile)
            else:
                self.wfile.write(json.dumps(payload).encode())
