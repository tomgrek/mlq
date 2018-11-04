import argparse
import asyncio
import functools
import time

from mlq import MLQ


parser = argparse.ArgumentParser(description='Controller app for MLQ')
parser.add_argument('cmd', metavar='command', type=str,
                    help='Command to run.', choices=['test_producer', \
                    'test_consumer', 'test_reaper', 'test_all', 'clear_all'])
parser.add_argument('--redis_host', default='localhost',
                    help='Hostname for the Redis backend, default to localhost')
parser.add_argument('--namespace', default='mlq_default',
                    help='Namespace of the queue')

def my_producer_func(q):
    while True:
        time.sleep(4)
        q.post('ZIG')

def my_consumer_func(msg, *args):
    print(msg)
    print('i was called! worker {} '.format(args[0]['worker']))
    print('prcessing started at {}'.format(args[0]['processing_started']))
    time.sleep(10)

async def main(command, namespace, redis_host):
    mlq = MLQ(namespace, redis_host, 6379, 0)
    if args.cmd == 'clear_all':
        print('Clearing everything in namespace {}'.format(namespace))
        for key in mlq.redis.scan_iter("{}*".format(namespace)):
            mlq.redis.delete(key)
    elif args.cmd == 'test_consumer':
        print('Starting test consumer')
        mlq.create_listener(my_consumer_func)
    elif args.cmd == 'test_producer':
        print('Starting test producer')
        mlq.loop.run_in_executor(mlq.pool, functools.partial(my_producer_func, mlq))
    elif args.cmd == 'test_reaper':
         print('Starting test reaper')
         mlq.create_reaper()
    elif args.cmd == 'test_all':
         print('Starting all dummy services')
         mlq.create_listener(my_consumer_func)
         mlq.loop.run_in_executor(mlq.pool, functools.partial(my_producer_func, mlq))
         mlq.create_reaper()

if __name__ == '__main__':
    args = parser.parse_args()
    if args.cmd:
        asyncio.run(main(args.cmd, args.namespace, args.redis_host))
