import asyncio
import functools
import time

from mlq import MLQ

def my_producer_func(q):
    while True:
        time.sleep(4)
        q.post('ZIG')

def my_consumer_func(msg, *args):
    print(msg)
    print('i was called! worker {} '.format(args[0]['worker']))
    print('prcessing started at {}'.format(args[0]['processing_started']))
    time.sleep(10)

async def main():
        mlq = MLQ('tom', 'localhost', 6379, 0)
        mlq.create_listener(my_consumer_func)
        mlq.create_reaper()
        print('Starting Producer Job')
        mlq.loop.run_in_executor(mlq.pool, functools.partial(my_producer_func, mlq))

asyncio.run(main())
