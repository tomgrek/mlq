# MLQ, a queue for ML jobs

## Installing

`pip install -r requirements.txt`

## Testing

`./run_tests.sh`

## Usage

Launch a test queue and listener with `python3 controller/app.py test_consumer --server`

Define a function in python and add it to the consumer's listener with:

`http.request('POST', 'localhost:5001/consumer', headers={'Content-Type':
     ...: 'application/json'}, body=jsonpickle.encode(cloudpickle.dumps(my_function)))`

Run a dummy app to see the callbacks happening:

`python test_callback.py`

Post something to the queue with

`python controller/app.py post "a message" "localhost:5000/callbackurl"`

Get a job result with

`curl localhost:5001/jobs/53/result --output -`

### Binary job results

Are supported. Results are available at e.g. `localhost:5001/jobs/53/result` but
also at `localhost:5001/jobs/53/result.jpg`, `localhost:5001/jobs/53/result.mp3`,
etc, so you can get a user's browser to interpret the binary in whatever way.
