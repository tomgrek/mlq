import argparse
import asyncio
import functools
import logging
from threading import Thread
import time

from flask import Flask, request

from mlq.queue import MLQ


parser = argparse.ArgumentParser(description='Controller app for MLQ')
parser.add_argument('cmd', metavar='command', type=str,
                    help='Command to run.', choices=['test_producer', \
                    'test_consumer', 'test_reaper', 'test_all', 'clear_all',
                    'post'])
parser.add_argument('msg', metavar='message', type=str,
                    help='Message to post.', nargs='?')
parser.add_argument('callback', metavar='callback', type=str,
                    help='URL to callback when job completes.', nargs='?')
parser.add_argument('--redis_host', default='localhost',
                    help='Hostname for the Redis backend, default to localhost')
parser.add_argument('--namespace', default='mlq_default',
                    help='Namespace of the queue')
parser.add_argument('--server', action='store_true',
                    help='Run a server')

def my_producer_func(q):
    while True:
        time.sleep(4)
        q.post('ZIG')

def my_consumer_func(msg, *args):
    print(msg)
    print('i was called! worker {} '.format(args[0]['worker']))
    print('prcessing started at {}'.format(args[0]['processing_started']))
    time.sleep(2)

def server(mlq):
    app = Flask(__name__)
    @app.route('/healthz')
    def healthz():
        return 'ok'
    @app.route('/jobs/count')
    def jobs_count():
        return str(mlq.job_count())
    @app.route('/jobs', methods=['POST'])
    def post_msg():
        resp = mlq.post(request.json["msg"])
        return str(resp)
    @app.route('/consumer', methods=['POST', 'DELETE'])
    def consumer_index_routes():
        if request.method == 'POST':
            return str(mlq.create_listener(request.json))
        if request.method == 'DELETE':
            return str(mlq.remove_listener(request.json))
    app.run()

async def main(args):
    mlq = MLQ(args.namespace, args.redis_host, 6379, 0)
    command = args.cmd
    if command == 'clear_all':
        print('Clearing everything in namespace {}'.format(args.namespace))
        for key in mlq.redis.scan_iter("{}*".format(args.namespace)):
            mlq.redis.delete(key)
    elif command == 'test_consumer':
        print('Starting test consumer')
        mlq.create_listener(my_consumer_func)
    elif command == 'test_producer':
        print('Starting test producer')
        mlq.loop.run_in_executor(mlq.pool, functools.partial(my_producer_func, mlq))
    elif command == 'test_reaper':
         print('Starting test reaper')
         mlq.create_reaper()
    elif command == 'test_all':
         print('Starting all dummy services')
         mlq.create_listener(my_consumer_func)
         mlq.loop.run_in_executor(mlq.pool, functools.partial(my_producer_func, mlq))
         mlq.create_reaper()
    elif command == 'post':
        mlq.post(args.msg, args.callback)
    if args.server:
        thread = Thread(target=server, args=[mlq])
        thread.start()
    return mlq

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    args = parser.parse_args()
    if args.cmd:
        asyncio.run(main(args))
