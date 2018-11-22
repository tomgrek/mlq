import argparse
import asyncio
import pytest
import signal
import multiprocessing
import time

import urllib3
import gevent

from controller.app import main, set_args
from controller.app import server as mlq_api_server
from mlq.queue import MLQ

@pytest.fixture
def http():
    return urllib3.PoolManager()

@pytest.fixture
def mlq():
    return MLQ('test_mlq_ns', 'localhost', 6379, 0)

is_server_running = False
proc = None

@pytest.fixture
def server():
    global is_server_running
    global proc
    if is_server_running:
        return is_server_running
    svr = mlq_api_server(mlq, '127.0.0.1', 4999, start_serving=False)
    def dorun():
        svr.run(host='127.0.0.1', port=4999, debug=True, use_reloader=False)
    proc = multiprocessing.Process(target=dorun)
    proc.start()
    time.sleep(2)
    is_server_running = svr
    yield svr
    proc.terminate()

def test_import():
    assert main

def test_args_namespace():
    args = set_args()
    args = args.parse_args(['dummy'])
    assert args.namespace == 'mlq_default'

def test_q_set():
    args = set_args()
    args = args.parse_args(['dummy'])
    q = asyncio.run(main(args))
    result = q._redis.set('test_mlq_ns:test_mlq_key', 'An artefact from MLQs tests')
    assert result
    assert q._redis.get('test_mlq_ns:test_mlq_key').decode('utf-8') == 'An artefact from MLQs tests'

def test_clear_all():
    args = set_args()
    args = args.parse_args(['clear_all', '--redis_host', 'localhost', '--redis_port', '6379', '--namespace', 'test_mlq_ns'])
    q = asyncio.run(main(args))
    keys = q._redis.keys('test_mlq_ns*')
    assert isinstance(keys, list)
    assert len(keys) == 0
    assert not q._redis.get('test_mlq_ns:test_mlq_key')

def test_server_status(http, mlq, server):
    resp = http.request('GET', 'http://localhost:4999/healthz')
    assert resp.status == 200
