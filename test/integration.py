from time import sleep

import pytest
import requests


def add_new_job_and_get_status(concurrency, url, delay=None) -> dict:
    job_id = requests.get('http://localhost:8888?concurrency=%s&sort=%s' % (concurrency, url)).json()['jobid']
    if delay:
        sleep(delay)
    return requests.get('http://localhost:8888?get=%s' % job_id).json()


def test_if_job_not_exist():
    result = requests.get('http://localhost:8888?get=hello').json()
    assert result['state'] == 'eexist'
    assert result['data'] is None


@pytest.mark.parametrize('concurrency, url, message', (
    ('hello', 'hello', 'concurrency must be integer'),
    (-10, 'hello', 'concurrency must be positive'),
    (0, 'hello', 'concurrency must be positive'),
    (1, 'http://[', 'invalid URL'),
    (1, 'https://example.com', 'URL must be a HTTP resource'),
))
def test_if_invalid_request(concurrency, url, message):
    result = add_new_job_and_get_status(concurrency, url)
    assert result['state'] == 'error'
    assert result['data'] == message


@pytest.mark.skip(reason='server should be started with workers=5')
def test_if_job_state_queued():
    for _ in range(5):
        add_new_job_and_get_status(1, 'http://localhost:8889/slow')
    result = add_new_job_and_get_status(1, 'http://localhost:8889/slow')
    assert result['state'] == 'queued'
    assert result['data'] is None


def test_if_job_state_progress():
    result = add_new_job_and_get_status(1, 'http://localhost:8889/slow')
    assert result['state'] == 'progress'
    assert result['data'] is None


@pytest.mark.parametrize('url, message', (
    ('http://localhost:8889/foobar', 'remote error'),
    ('http://localhost:8889/empty', 'data seems empty'),
    ('http://localhost:8889/incorrect', 'incorrect data'),
    ('http://localhost:8887', 'unexpected error'),
))
def test_if_bad_remote_data(url, message):
    result = add_new_job_and_get_status(3, url, 1)
    assert result['state'] == 'error'
    assert result['data'] == message


def test_if_number_amount_less_than_concurrency():
    result = add_new_job_and_get_status(20, 'http://localhost:8889/amount/10', 1)
    assert result['state'] == 'ready'
    assert result['data'] is not None
    assert len(result['data']) == 10
    assert result['data'] == sorted(result['data'])


def test_if_number_amount_greater_than_concurrency():
    result = add_new_job_and_get_status(3, 'http://localhost:8889/amount/1000', 1)
    assert result['state'] == 'ready'
    assert result['data'] is not None
    assert len(result['data']) == 1000
    assert result['data'] == sorted(result['data'])
