import asyncio
import time

from mlq.queue import MLQ

mlq = MLQ('example_app', 'localhost', 6379, 0)

def listener_func(number):
    print(number)
    time.sleep(5)
    return number ** 2

async def main():
    print("Running, waiting for messages.")
    mlq.create_listener(listener_func)

if __name__ == '__main__':
    asyncio.run(main())
