# MLQ, a queue for ML jobs

You've got an ML model and want to deploy it. Meaning that, you have a web app
and want users to be able to re-train the model, and that takes a long time. Or
perhaps even inference takes a long time. Long, relative to the responsiveness
users expect from webapps, meaning, not immediate.

The solution is to enqueue the user's request, and until then, show the user
some loading screen, or tell them to check back in a few minutes. The ML stuff
happens in the background, and when it's done, the user is notified (maybe via
  websockets; maybe their browser is polling at intervals).

Or perhaps your company has a limited resource, such as GPUs, and you need a solution
for employees to access them from Jupyter one-by-one.

MLQ is designed to provide a performant, reliable, and most of all easy to use, queue and
workers to solve the above common problems.

It's in Python 3.6+, is built on asyncio, and uses Redis as a queue backend.

## Installing

`pip install -r requirements.txt`

## Testing

`./run_tests.sh`

## Usage as a fixed backend

## Usage over HTTP as a more flexible queue/worker system

MLQ uses [gevent](http://www.gevent.org/index.html) (similar to gunicorn if you know that better) for a WSGI server.

If you want to launch many servers -- which may not be necessary, you probably want one
server but many workers -- don't try any special `gevent` magic, just see below.

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

Note that if you are expanding `server_address` beyond 127.0.0.1, MLQ does not come
with any authentication. It'd be open to the world (or the local reachable cluster/network).

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

Functions sent to workers should be unique by name. If you later decide you don't need
that function any more, just make the same request but instead of POST, use DELETE.

3. Send a message to the server

If you're using the app tool, it's very easy:

```
python controller/app.py post '{"arg1": 6, "arg2": 3}'
```

If you're doing it from `curl` or direct from Python, you have to bear in mind that
you need to POST a string which is a JSON object with at least a `msg` key. So, for example:

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

Once processing is complete, if you passed a "callback" key in the POST body, that callback
will be called with the (short) result and a success flag. But we didn't do that yet, so ...

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


### Can messages be lost?

MLQ is designed with atomic transactions such that queued messages should not be lost (of course
  this cannot be guaranteed).

There is always the possibility that the backend Redis instance will go down, if
you are concerned about this, I recommend looking into Redis AOF persistence.

### Binary job results

Are supported. Results are available at e.g. `localhost:5001/jobs/53/result` but
also at `localhost:5001/jobs/53/result.jpg`, `localhost:5001/jobs/53/result.mp3`,
etc, so you can get a user's browser to interpret the binary in whatever way.
