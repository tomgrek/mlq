# MLQ, a queue for ML jobs

MLQ is a job queueing system, and framework for workers to process queued jobs, providing an easy way to offload long running jobs to other computers.

You've got an ML model and want to deploy it. Meaning that, you have a web app and want users to be able to re-train the model, and that takes a long time. Or perhaps even inference takes a long time. Long, relative to the responsiveness users expect from webapps, meaning, not immediate.

You can't do this stuff direct from your Flask app, because it would lock up the app and not scale beyond a couple of users.

The solution is to enqueue the user's request, and until then, show the user some loading screen, or tell them to check back in a few minutes. The ML stuff happens in the background, in a separate process, or perhaps on a different machine. When it's done, the user is notified (maybe via websockets; maybe their browser is polling at intervals).

Or perhaps your company has a limited resource, such as GPUs, and you need a solution for employees to access them from Jupyter one-by-one.

MLQ is designed to provide a performant, reliable, and most of all easy to use, queue and workers to solve the above common problems.

It's in Python 3.6+, is built on asyncio, and uses Redis as a queue backend.

## Usage

`pip install mlq`

## Requirements

You need access to a running Redis instance, for example `apt install redis-server` will get you one at localhost:6379, otherwise there is AWS's Elasticache and many other options.

## Quick Start

This assumes: you have a web app with a Python backend. For a complete example, see [here](https://github.com/tomgrek/mlq/tree/master/examples/full_workflow). In brief:

```
import time
from mlq.queue import MLQ

# Create MLQ: namespace, redis host, redis port, redis db
mlq = MLQ('example_app', 'localhost', 6379, 0)

job_id = mlq.post({'my_data': 1234})
result = None
while not result:
    time.sleep(0.1)
    job = mlq.get_job(job_id)
    result = job['result']
```

Then, of course you need a worker, or many workers, processing the job. Somewhere else (another terminal, a screen session, another machine, etc):

```
import asyncio
from mlq.queue import MLQ

mlq = MLQ('example_app', 'localhost', 6379, 0)

def simple_multiply(params_dict, *args):
    return params_dict['my_data'] * 2

async def main():
    print("Running, waiting for messages.")
    mlq.create_listener(simple_multiply)

if __name__ == '__main__':
    asyncio.run(main())
```

## Job Lifecycle

1. Submit a job with MLQ. Optionally, specify a callback URL that'll be hit, with some useful query params, once the job has been processed.
2. Job goes into a queue managed by MLQ. Jobs are processed first-in, first-out (FIFO).
3. Create a worker (or maybe a worker already exists). Optimally, create many workers. They all connect to a shared Redis instance.
4. A worker (sorry, sometimes I call it a consumer), or many workers in parallel, will pick jobs out of the queue and process them by feeding them into listener functions that you define.
5. As soon as a worker takes on the processing of some message/data, that message/data is moved into a processing queue. So, if the worker fails midway through, the message is not lost.
6. Worker stores the output of its listener functions and hits the callback with a result. Optionally, a larger result -- perhaps binary -- is stored in Redis, waiting to be picked up and served by a backend API.
7. Ask MLQ for the job result, if the callback was not enough for you.

Alternatively, the worker might fail to process the job before it gets to step 6. Maybe the input data was invalid, maybe it was a bad listener function; whatever happened, there was an exception:

8. MLQ will move failed jobs into a dead letter queue - not lost, but waiting for you to fix the problem.

In another case, maybe the worker dies midway through processing a message. When that happens, also the job is not lost. It might just be a case of, for example, the spot instance price just jumped and your worker shut down. MLQ provides a [reaper](https://github.com/tomgrek/mlq#the-reaper) thread that can be run at regular intervals to requeue jobs whose processing has stalled. If the job is requeued enough times that it exceeds some threshold you've specified, something is wrong - it'll be moved to the dead letter queue.

## Listener functions: arguments and return values

When creating a worker, you (probably should) give it a listener function to execute on items pulled from the queue. In the example above, that function is called `simple_multiply`. Its signature is `simple_multiply(params_dict, *args)`. What is that?

* `params_dict` is the original message that was pushed to the queue; in the example it'd be this dictionary: `{'my_data': 1234}`. It doesn't have to be a dict, rather, it's whatever you `post`ed to the queue, so it could be an `int` or a `string`; any serializable type (it needs to be serializable because in the Redis queue it has to be stored as a string). Speaking of which, internally, MLQ does its serialization with [MessagePack](https://msgpack.org/index.html), which supports binary and is faster than JSON.

* `args` is a dict with several useful things in. Most of them are concerned with distributed computing utilities (documented below) and so can safely be ignored by most users. But, you also get access to the dict `args['full_message']` and the function `args['update_progress']`. The full message provides the queued message as MLQ sees it, including some possibly useful things:

```
{
    'id': msg_id,
    'timestamp': timestamp, # when the message was enqueued
    'worker': None, # uuid of the worker it's being processed on (if any)
    'processing_started': None, # when processing started on the worker
    'processing_finished': None,
    'progress': None, # an integer from -1 (failed), 0 (not started), to 100 (complete)
    'short_result': None, # the stored short result (if any)
    'result': None, # the stored full result (if any)
    'callback': callback, # URL to callback, if any, that user specified when posting the message
    'retries': 0, # how many times this job has attempted to be processed by workers and requeued
    'functions': functions, # which listener function names should be called, default all of them
    'msg': msg # the actual message
}
```

After a worker completes processing, this same object (a serialized version of it, anyway) remains stored in Redis and (the de-serialized version of it) accessible using `mlq.get_result(job_id)`.

You can update the progress of a message from within your listener function like so: `args['update_progress'](50)` where `50` represents 50% (but you can update with any stringify-able type). This is useful say if processing a job has two time consuming steps. So if you have a UI that's polling the backend for updates, after completing one of them, set progress to 50% - then the responsible backend endpoint can just call `mlq.get_progress(job_id)` (where `job_id` is the UUID that was returned from `mlq.post`) and it will get that 50% complete value which you can report to the client.

#### The return value

Remember that if you write a listener function that imports other libraries, they need to be importable on whatever machines/Python environments the consumers are running on, too.

## Usage over HTTP

MLQ uses [gevent](http://www.gevent.org/index.html) (similar to gunicorn if you know that better) for a WSGI server.

If you want to launch many servers -- which may not be necessary, you probably want one server but many workers -- there's no need for any special `gevent` magic.

1. Launch one (or more) consumers:

`python3 controller/app.py consumer --server`

There may be little benefit to running more than one server unless you are
going to put some load balancer in front of them _and_ are expecting seriously
high traffic. To run workers that don't have an HTTP interface -- they will
still get messages that are sent to the first `--server` worker -- just leave
off the `--server` parameter.

Optional params:
```
--server_address 0.0.0.0 (if you want it to accept connections from outside localhost)
--server_port 5000
```

Note that if you are expanding `server_address` beyond 127.0.0.1, MLQ does not come with any authentication. It'd be open to the world (or the local reachable cluster/network).

For multiple servers, you'll need to specify each with a different `server_port`. Nothing
is automagically spun off into different threads. One server, one process, one port. Load
balance it via nginx or your cloud provider, if you really need this. Hint: you probably don't.

2. Give the consumer something to do

The consumer sits around waiting for messages, and when it receives one, runs the
listener functions that you've specified on that message. The output of the last
listener function is then stored as the result. So, you need to specify at least
one listener function (unless that is you just want a queue for, say, logging purposes).

Define a function:
```
def multiply(msg, *args):
    print("I got a message: " + str(msg))
    return msg['arg1'] * msg['arg2']
```

`msg` should be a string or dictionary type. The `args` dictionary gives
access to the full message as-enqueued, including its id, the UUID of the worker that's
processing it, the timestamp of when it was enqueued, as well as a bunch of utility functions
useful for distributed processing. See later in this document.

When you have the function defined, you need to `cloudpickle` then `jsonpickle` it to send it
to the consumer as a HTTP POST. In Python, the code would be something like:

```
import cloudpickle
import jsonpickle
import urllib3
http = urllib3.PoolManager()
http.request('POST', 'localhost:5001/consumer',
             headers={'Content-Type':'application/json'},
             body=jsonpickle.encode(cloudpickle.dumps(multiply)))
```

Functions sent to workers should be unique by name. If you later decide you don't need that function any more, just make the same request but instead of POST, use DELETE.

3. Send a message to the server

If you're using the app tool, it's very easy:

```
python controller/app.py post '{"arg1": 6, "arg2": 3}'
```

If you're doing it from `curl` or direct from Python, you have to bear in mind that you need to POST a string which is a JSON object with at least a `msg` key. So, for example:

```
r = http.request('POST', 'localhost:5001/jobs',
                 headers={'Content-Type':'application/json'},
                 body=json.dumps({'msg':{'arg1':6, 'arg2':3}}))
```

From curl:
```
curl localhost:5001/jobs -X POST -d '{"msg": {"arg1": 6, "arg2": 3}}' -H 'Content-Type: application/json'
```

You'll get back the unique id of the enqueued request, which is an integer.

Once processing is complete, if you passed a "callback" key in the POST body, that callback will be called with the (short) result and a success flag. But we didn't do that yet, so ...

4. ... at some point in the future, get the result.

Results are stored indefinitely. Retrieve it by curl with something like:

```
curl localhost:5001/jobs/53/result
```

MLQ supports binary results, but to see them with curl you'll need to add ` --output -`. Because
results are binary, they are returned just as bytes, so beware string comparisons and suchlike. Alternatively,
you can hit the `/short_result` endpoint; `short_result` is always a string.

To get the result within Python, it's just:

```
r = http.request('GET', 'localhost:5001/jobs/53/result')
print(r.data)
```

Additionally, say for example your job's result was a generated image or waveform. Binary data,
but you want it to be interpreted by the browser as say a JPG or WAV. You can add any
file extension to the above URL so a user's browser would interpret it correctly. For example,
the following are equivalent:
```
curl localhost:5001/jobs/53/result
curl localhost:5001/jobs/53/result.mp3
```

5. Add a callback

When you enqueue a job, you can optionally pass a callback which will be called when
the job is complete. Here, callback means a URL which will be HTTP GET'd. The URL will be
attempted up to 3 times before MLQ gives up.

The user submits a job via your backend (Node, Flask, whatever), you return immediately --
but create within your backend a callback URL that when it's hit, knows to signal
something to the user.

You pass the callback URL when you enqueue a job. It will receive the following arguments
as a query string: `?success=[success]&job_id=[job_id]&short_result=[short_result]`

* Success is 0 or 1 depending whether the listener functions errored (threw exceptions) or timeout'd, or if everything went smoothly.

* Job ID is the ID of the job you received when you first submitted it. This makes it
possible to have a single callback URL defined in your app, and handle callbacks
dependent on the job id.

* Short result is a string that can become part of a URL, returned from your listener
function. For example, if you do some image processing and determine that the picture
shows a 'widget', `short_result` might be `widget`.

The end result is that you'll get a callback to something like
[whatever URL you passed]?success=1&job_id=53&short_result=widget.

It's possible three ways, depending if you're using the client app, Python directly, or HTTP calls:
```
mlq.post('message', callback='http://localhost:5001/some_callback')
python3 controller/app.py post message --callback 'http://localhost:5001/some_callback'
curl localhost:5001/jobs -X POST -H 'Content-Type: application/json' -d '{"msg":"message", "callback":'http://localhost:5001/some_callback'}'
```

## The reaper

If a worker goes offline mid-job, that job would get stuck in limbo: it looks like someone picked it up, but it never finished. MLQ comes with a reaper that detects jobs that have been running for longer than a threshold, assumes the worker probably died, and requeues the job. It keeps a tally
of how many times the job was requeued; if that's higher than some specified number, it'll move the job to the dead letter queue instead of requeuing it.

The reaper runs at a specified interval. To create a reaper that runs once every 60s, checks for jobs running for longer than 300s (five minutes), and if they've been retried fewer than 5 times puts them back in the job queue:

```
If you're using MLQ directly from Python:
mlq.create_reaper(call_how_often=60, job_timeout=300, max_retries=5)

If you're using MLQ from the command line:
python3 controller/app.py --reaper --reaper_interval 60 --reaper_timeout 300 --reaper_retries 5
```

## Distributed Computing

To aid in distributed computing, there are a number of additional conveniences in MLQ.

* Specify what listener functions are called for a job.

Normally, you add listener functions to a list, and they are all called any time a message
comes in. In fact, 99% of the time you'll probably just add a single listener function to
each consumer.

That's not always desirable though: maybe from one listener function, you want to enqueue a partial result that's handled in another worker or two. (Workers share all listener functions; at least the ones that were added since they came into existence.)

To this end, you can specify a list of functions that are called for a particular message. Only _those_ functions will be called when the message is dequeued into a listener function.

As always, there are 3 possible ways to do it: pure Python, with the controller app, and via http.
```
mlq.post('message', functions=['my_func'])
python3 controller/app.py post message --functions my_func
curl localhost:5001/jobs -X POST -H 'Content-Type: application/json' -d '{"msg":"message", "functions":["my_func"]}'
```

* Store data and fetch data

Listener functions can store and fetch data using Redis as the backend. This way, they can pass data between different workers perhaps on different machines: say you have a graph where one computation generates a single interim result that can then be simultaneously worked on by two other workers.

These functions are passed as arguments to the listener function:

`key = store_data(data, key=None, expiry=None)`: Data must be a string type. Key is the key
in Redis that the data is stored at, leave at None to have MLQ generate a UUID for you. Expiry
of the key (in seconds) is recommended. Returns the key at which data was stored.

`data = fetch_data(data_key)`: Retrieve data stored in Redis at `data_key`.

* Post a message to the queue from within a listener function

The API is the same from within a listener function as it is for using the MLQ library directly: `job_id = post(msg, callback=None, functions=None)`

* Block a listener function until the output from another listener function is available

`result = block_until_result(job_id)`. Be careful using this if you have only one worker because all of the worker's execution will be blocked: it won't suspend, process the other job, and return.

### Can messages be lost?

MLQ is designed with atomic transactions such that queued messages should not be lost (of course this cannot be guaranteed).

There is always the possibility that the backend Redis instance will go down, if you are concerned about this, I recommend looking into Redis AOF persistence.

### Binary job results

Are supported. Results are available at e.g. `localhost:5001/jobs/53/result` but
also at `localhost:5001/jobs/53/result.jpg`, `localhost:5001/jobs/53/result.mp3`,
etc, so you can get a user's browser to interpret the binary in whatever way.

### Why not Kafka, Celery, Dask, 0MQ, RabbitMQ, etc

Primarily, simplicity and ease of use. While MLQ absolutely is suitable for a production system, it's easy and intuitive to get started with on a side project.

Dask and Celery are excellent, use them in preference to MLQ if you want to invest the time.

### Developing

```
git clone https://github.com/tomgrek/mlq.git
pip install -r requirements.txt
source ./run_tests.sh
python setup.py sdist upload
```
