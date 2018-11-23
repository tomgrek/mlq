import asyncio
from mlq.queue import MLQ

mlq = MLQ('example', 'localhost', 6379, 0)

def some_listener_func(params, *args):
    return params

def main():
    print("Worker starting")
    async def start_worker():
        mlq.create_listener(some_listener_func)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_worker())

if __name__ == '__main__':
    main()
