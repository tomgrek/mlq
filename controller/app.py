import argparse
import asyncio
import functools
import logging
from threading import Thread
import time

from flask import Flask, request
from gevent.pywsgi import WSGIServer
import msgpack
from werkzeug.exceptions import NotFound

from mlq.queue import MLQ

def set_args():
    parser = argparse.ArgumentParser(description='Controller app for MLQ')
    parser.add_argument('cmd', metavar='command', type=str,
                        help='Command to run.', choices=['test_producer', \
                        'test_consumer', 'test_reaper', 'test_all', 'clear_all',
                        'post', 'consumer', 'dummy'])
    parser.add_argument('msg', metavar='message', type=str,
                        help='Message to post.', nargs='?')
    parser.add_argument('--callback', metavar='callback',
                        help='URL to callback when job completes.')
    parser.add_argument('--functions', metavar='functions',
                        help='List of function names to call for this message, default all.', nargs='+')
    parser.add_argument('--redis_host', default='localhost',
                        help='Hostname for the Redis backend, default to localhost')
    parser.add_argument('--redis_port', default=6379,
                        help='Port for the Redis backend, default to 6379')
    parser.add_argument('--namespace', default='mlq_default',
                        help='Namespace of the queue')
    parser.add_argument('--reaper', action='store_true',
                        help='Run the reaper to harvest stalled jobs')
    parser.add_argument('--reaper_interval', default=60,
                        help='How often in seconds that the reaper should check for stalled jobs.')
    parser.add_argument('--reaper_timeout', default=300,
                        help='How long a job should be let run before assuming it failed and re-queueing.')
    parser.add_argument('--reaper_retries', default=5,
                        help='How many times reaper should requeue a job before moving it to dead letter queue.')
    parser.add_argument('--server', action='store_true',
                        help='Run a server')
    parser.add_argument('--server_address', default='127.0.0.1',
                        help='Address for server to bind to. 127.0.0.1 is default which listens only on localhost, specify 0.0.0.0 to listen to all.')
    parser.add_argument('--server_port', default=5000,
                        help='Port for server to listen at.')
    return parser

def my_producer_func(q):
    while True:
        time.sleep(4)
        q.post('a random message')

def simple_consumer_func(msg, *args):
    time.sleep(4)
    return (msg + 'was processed')

def my_consumer_func(msg, *args):
    utils = args[0]
    print(msg)
    print('i was called! worker {} '.format(utils['full_message']['worker']))
    print('prcessing started at {}'.format(utils['full_message']['processing_started']))
    data_id = utils['store_data']('some data to be stored and expire in 100 seconds', expiry=100)
    print(data_id)
    time.sleep(3)
    print(utils['fetch_data'](data_id))
    utils['update_progress'](5)
    time.sleep(3)
    utils['update_progress'](56)
    time.sleep(3)
    new_msg_id = utils['post']('new message from within!', functions=['simple_consumer_func'])
    print(new_msg_id)
    print('I would be returning right now, except that.. the other job')
    other_job_result = utils['block_until_result'](new_msg_id)
    print(other_job_result)
    # return ('a short success', 'a longer success')
    return other_job_result

def server(mlq, address, port, start_serving=True):
    flask_app = Flask(__name__)
    @flask_app.route('/healthz')
    def healthz():
        return 'ok'
    @flask_app.route('/jobs/count')
    def jobs_count():
        return str(mlq.job_count())
    @flask_app.route('/jobs', methods=['POST'])
    def post_msg():
        msg = request.json.get('msg', None)
        callback = request.json.get('callback', None)
        functions = request.json.get('functions', None)
        resp = mlq.post(msg, callback, functions)
        return str(resp)
    @flask_app.route('/jobs/<job_id>/progress', methods=['GET'])
    def get_progress(job_id):
        return mlq.get_progress(job_id)
    @flask_app.route('/jobs/<job_id>/short_result', methods=['GET'])
    def get_short_result(job_id):
        job = mlq._redis.get(mlq.progress_q + '_' + job_id)
        job = msgpack.unpackb(job, raw=False)
        return str(job['short_result']) or '[no result]'
    @flask_app.route('/jobs/<job_id>/result', defaults={'extension': None}, methods=['GET'])
    @flask_app.route('/jobs/<job_id>/result<extension>', methods=['GET'])
    def get_result(job_id, extension):
        job = mlq._redis.get(mlq.progress_q + '_' + job_id)
        if not job:
            raise NotFound
        try:
            job = msgpack.unpackb(job, raw=False)
            return str(job['result']) or '[no result]'
        except UnicodeDecodeError:
            job = msgpack.unpackb(job, raw=True)
            return job[b'result'] or '[no result]'
    @flask_app.route('/consumer', methods=['POST', 'DELETE'])
    def consumer_index_routes():
        if request.method == 'POST':
            return str(mlq.create_listener(request.json))
        if request.method == 'DELETE':
            return str(mlq.remove_listener(request.json))
    if start_serving:
        http_server = WSGIServer((address, int(port)), flask_app)
        logging.info('Serving at {} port {}'.format(address, port))
        http_server.serve_forever()
    else:
        return flask_app

async def main(args):
    mlq = MLQ(args.namespace, args.redis_host, int(args.redis_port), 0)
    command = args.cmd
    if command == 'clear_all':
        print('Clearing everything in namespace {}'.format(args.namespace))
        for key in mlq._redis.scan_iter("{}*".format(args.namespace)):
            mlq._redis.delete(key)
    elif command == 'consumer':
        mlq.create_listener()
    elif command == 'test_consumer':
        print('Starting test consumer')
        mlq.create_listener(simple_consumer_func)
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
        print('Posting message to queue.')
        mlq.post(args.msg, args.callback, args.functions)
    if args.reaper:
        mlq.create_reaper(args.reaper_interval, args.reaper_timeout, args.reaper_retries)
    if args.server:
        thread = Thread(target=server, args=[mlq, args.server_address, args.server_port])
        thread.start()
    return mlq

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = set_args()
    args = parser.parse_args()
    if args.cmd:
        asyncio.run(main(args))
