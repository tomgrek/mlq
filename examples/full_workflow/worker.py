import asyncio
import time

from mlq.queue import MLQ

mlq = MLQ('example_app', 'localhost', 6379, 0)

def listener_func(number_dict, *args):
    print(number_dict['number'])
    time.sleep(10)
    return number_dict['number'] ** 2

async def main():
    print("Running, waiting for messages.")
    mlq.create_listener(listener_func)

if __name__ == '__main__':
    asyncio.run(main())
