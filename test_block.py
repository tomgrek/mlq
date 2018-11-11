import redis
import time
import asyncio
import concurrent

red = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
sub = red.pubsub(ignore_subscribe_messages=True)
pool = concurrent.futures.ThreadPoolExecutor()

async def waiter():
    sub.unsubscribe()
    sub.subscribe('garbage')
    while True:
        msg = sub.get_message()
        if msg:
            return msg['data']
        time.sleep(0.001)
        await asyncio.sleep(0.01)

def looper():
    while True:
        print('zig')
        time.sleep(2)

async def main():
    print('asd')
    loop = asyncio.get_running_loop()
    #r = loop.run_in_executor(pool, waiter)
    r2 = loop.run_in_executor(pool, looper)
    r = asyncio.create_task(waiter())
    await r

    print('hoho', r, r.result())

if __name__=='__main__':
    print('main')
    asyncio.run(main())
