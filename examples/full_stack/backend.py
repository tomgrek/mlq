"""This is a simple example application backend.
It provides an endpoint for the user to call to request some processing, a status endpoint
through which the user can poll the status of their job, and a callback endpoint which
is called by the worker, useful if we wanted to push a notification to the user."""

from flask import Flask, request, jsonify

from mlq.queue import MLQ

app = Flask(__name__)

# Create MLQ: namespace, redis host, redis port, redis db
mlq = MLQ('example_app', 'localhost', 6379, 0)

CALLBACK_URL = 'http://localhost:3000/callback'

@app.route('/do_inference', methods=['POST'])
def do_fake_inference():
    # The worker is configured to just return the passed-in number, squared.
    # With a 30s delay to make it somewhat realistic!
    assert request.json['number']
    job_id = mlq.post(request.json, CALLBACK_URL)
    return jsonify({'msg': 'Processing, check back soon.', 'job_id': job_id})

@app.route('/status/<job_id>', methods=['GET'])
def get_progress(job_id):
    return jsonify({'msg': mlq.get_progress(job_id)})

@app.route('/callback', methods=['GET'])
def train_model():
    success = request.args.get('success', None)
    job_id = request.args.get('job_id', None)
    short_result = request.args.get('short_result', None)
    print("We received a callback! Job ID {} returned successful={} with short_result {}".format(
        job_id, success, short_result
    ))
    return 'ok'

app.run(host='localhost', port=3000)
