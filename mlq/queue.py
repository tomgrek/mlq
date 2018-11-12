import asyncio
import concurrent.futures
from datetime import datetime as dt
import functools
import logging
import json
import pickle
import urllib3
import time
from uuid import uuid1 as uuid

from werkzeug.exceptions import NotFound

import msgpack
import redis
import jsonpickle
import cloudpickle

class MLQ():
    """Create an MLQ object"""
    def __init__(self, q_name, redis_host, redis_port, redis_db):
        self.q_name = q_name
        self.processing_q = self.q_name + '_processing'
        self.job_status_stem = self.q_name + '_progress_'
        self.jobs_refs_q = self.q_name + '_jobsrefs'
        self.dead_letter_q = self.q_name + '_deadletter'
        # msgs have a 64 bit id starting at 0
        self.id_key = self.q_name + '_max_id'
        self.id = str(uuid())
        self._redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        logging.info('Connected to Redis at {}:{}'.format(redis_host, redis_port))
        self.pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        self.funcs_to_execute = []
        self.listener = None
        self.http = urllib3.PoolManager()
        self.loop = None
        self.pool = None

    def _create_async_stuff(self):
        self.loop = asyncio.get_running_loop()
        self.pool = concurrent.futures.ThreadPoolExecutor()

    def _utility_functions(self):
        """These utilities are passed to listener functions. They allow those
        functions to create new messages (e.g. so it can pipe the output of a listener
        function into another listener function which is potentially running on a different
        listener) and to update progress in case someone queries it midway through."""

        def update_progress(job_id, progress):
            """Update a job's progress. Progress goes from None (not started yet
            by any worker), 0 (job started and user hasn't updated progress), -1
            (job failed and moved to dead letter queue), to 100 (finished)."""
            progress_str = self._redis.get(self.job_status_stem + job_id)
            progress_dict = msgpack.unpackb(progress_str, raw=False)
            progress_dict['progress'] = progress
            new_record = msgpack.packb(progress_dict, use_bin_type=False)
            self._redis.set(self.job_status_stem + job_id, new_record)
            return True

        def store_data(data, key=None, expiry=None):
            """Stores data in Redis. You can pass a key or just leave it
            as none and have MLQ generate and return a unique key. Expiry
            is in seconds"""
            uid = key or uuid()
            self._redis.set(uid, data, ex=expiry)
            return uid

        def fetch_data(data_key):
            """Fetch data stored in Redis at `data_key`"""
            return self._redis.get(data_key)

        def post(msg, callback=None, functions=None):
            """Post a message to the queue"""
            return self.post(msg, callback, functions)

        def block_until_result(job_id):
            """Wait (within a listener function) until the result from another
            queued item is available. This is blocking and as a result, dangerous:
            if no other workers are available to process the inner message, the
            outer processing will be blocked; so you need at least 2 workers"""
            wait_key = 'pub_' + str(job_id)
            self.pubsub.unsubscribe()
            self.pubsub.subscribe(wait_key)
            while True:
                msg = self.pubsub.get_message()
                if msg:
                    return msg['data']
                time.sleep(0.001)

        return {
            'post': post,
            'update_progress': update_progress,
            'store_data': store_data,
            'fetch_data': fetch_data,
            'block_until_result': block_until_result,
        }

    def remove_listener(self, function):
        """Remove a function from the execution schedule of a worker upon msg.
        Workers' functions must be unique by name."""
        if isinstance(function, dict):
            fun_bytes = jsonpickle.decode(json.dumps(function))
            function = cloudpickle.loads(fun_bytes)
        for func in self.funcs_to_execute:
            if func.__name__ == function.__name__:
                self.funcs_to_execute.remove(func)
                return True
        return False

    def create_listener(self, function=None):
        """Create a MLQ consumer that executes `function` on message received.
        :param func function: A function with the signature my_func(msg, *args).
        Msg is the posted message, optional args[0] inside the function gives access
        to the entire message object (worker id, timestamp, retries etc)
        If there are multiple consumers, they get messages round-robin

        function can return a single item or a tuple of (short_result, long_result)
        short_result must be str, but long_result can be binary.
        """
        # TODO: Probably should be able to specify which worker will do what
        # functions. So also need an endpoint to get worker name.
        if not self.loop or not self.pool:
            self._create_async_stuff()

        if isinstance(function, dict):
            fun_bytes = jsonpickle.decode(json.dumps(function))
            function = cloudpickle.loads(fun_bytes)
        if function:
            self.funcs_to_execute.append(function)
        if self.listener:
            return True
        def listener():
            while True:
                msg_str = self._redis.brpoplpush(self.q_name, self.processing_q, timeout=0)
                msg_dict = msgpack.unpackb(msg_str, raw=False)
                msg_dict['worker'] = self.id
                msg_dict['processing_started'] = dt.timestamp(dt.utcnow())
                msg_dict['progress'] = 0
                new_record = msgpack.packb(msg_dict, use_bin_type=False)
                self._redis.set(self.job_status_stem + str(msg_dict['id']), new_record)
                all_ok = True
                short_result = None
                utils = self._utility_functions()
                utils['full_message'] = msg_dict
                utils['update_progress'] = functools.partial(utils['update_progress'], str(msg_dict['id']))
                result = None
                for func in self.funcs_to_execute:
                    if msg_dict['functions'] is None or func.__name__ in msg_dict['functions']:
                        try:
                            result = func(msg_dict['msg'], utils)
                        except Exception as e:
                            all_ok = False
                            logging.error(e)
                            logging.info("Moving message {} to dead letter queue".format(msg_dict['id']))
                            if msg_dict['callback']:
                                self.http.request('GET', msg_dict['callback'], fields={
                                    'success': 0,
                                    'job_id': str(msg_dict['id']),
                                    'short_result': None
                                })
                            msg_dict['progress'] = -1
                            msg_dict['result'] = str(e)
                            new_record = msgpack.packb(msg_dict, use_bin_type=False)
                            self._redis.set(self.job_status_stem + str(msg_dict['id']), new_record)
                            self._redis.rpush(self.dead_letter_q, msg_str)
                if all_ok:
                    logging.info('Completed job {}'.format(str(msg_dict['id'])))
                    msg_dict['worker'] = None
                    if result and type(result) in [tuple, list] and len(result) > 1:
                        short_result = result[0]
                        result = result[1]
                    else:
                        short_result = result
                    msg_dict['result'] = result
                    msg_dict['short_result'] = short_result
                    self._redis.publish('pub_' + str(msg_dict['id']), short_result)
                    msg_dict['progress'] = 100
                    msg_dict['processing_finished'] = dt.timestamp(dt.utcnow())
                    new_record = msgpack.packb(msg_dict, use_bin_type=False)
                    self._redis.set(self.job_status_stem + str(msg_dict['id']), new_record)
                    if msg_dict['callback']:
                        self.http.request('GET', msg_dict['callback'], fields={
                            'success': 1,
                            'job_id': str(msg_dict['id']),
                            'short_result': short_result
                        })
                self._redis.lrem(self.processing_q, -1, msg_str)
                self._redis.lrem(self.jobs_refs_q, 1, str(msg_dict['id']))
        logging.info('Created listener')
        self.listener = self.loop.run_in_executor(self.pool, listener)
        return True

    def job_count(self):
        """Returns the number of jobs in the queue, including both processing jobs and not-yet-processing"""
        return self._redis.llen(self.q_name)

    def create_reaper(self, call_how_often=60, job_timeout=300, max_retries=5):
        """A thread to reap jobs that were too slow
        :param int call_how_often: How often reaper should be called, every [this] seconds
        :param int job_timeout: Jobs processing for longer than this will be requeued
        :param int max_retries: How many times to requeue the message before moving it to dead letter queue"""
        if not self.loop or not self.pool:
            self._create_async_stuff()

        def reaper():
            while True:
                time.sleep(call_how_often)
                time_now = dt.timestamp(dt.utcnow())
                queued_jobs_length = self._redis.llen(self.jobs_refs_q)
                # check first 5 msgs in queue, if any exceed timeout, keep checking
                for i in range(0, (queued_jobs_length // 5) + 1, 5):
                    job_keys = self._redis.lrange(self.jobs_refs_q, i, i + 5)
                    all_ok = True
                    for job_key in job_keys:
                        progress_key = self.job_status_stem + job_key
                        job_str = self._redis.get(progress_key)
                        if not job_str:
                            logging.warning('Found orphan job {}'.format(job_key))
                            self._redis.lrem(self.jobs_refs_q, 1, job_key)
                            all_ok = False
                            continue
                        job = msgpack.unpackb(job_str, raw=False)
                        if job['progress'] != 100 and job['worker'] and time_now - job['processing_started'] > job_timeout:
                            logging.warning('Moved job id {} on worker {} back to queue after timeout {}'.format(job['id'], job['worker'], job_timeout))
                            pipeline = self._redis.pipeline()
                            job['processing_started'] = None
                            job['progress'] = None
                            job['worker'] = None
                            pipeline.lrem(self.processing_q, -1, msgpack.packb(job, use_bin_type=False))
                            job['timestamp'] = dt.timestamp(dt.utcnow())
                            job['retries'] += 1
                            job_id = job['id']
                            if job['retries'] >= max_retries:
                                pipeline.rpush(self.dead_letter_q, job['msg'])
                            else:
                                job = msgpack.packb(job, use_bin_type=False)
                                pipeline.set(self.job_status_stem + job_id, job)
                                pipeline.lpush(self.q_name, job)
                            pipeline.lrem(self.jobs_refs_q, 1, job_id)
                            pipeline.rpush(self.jobs_refs_q, job_id)
                            pipeline.execute()
                            all_ok = False
                            # TODO: call callback with failed + requeued status
                    if all_ok:
                        break
        self.loop.run_in_executor(self.pool, reaper)

    def get_job(self, job_id):
        job = self._redis.get(self.job_status_stem + job_id)
        job = msgpack.unpackb(job, raw=False)
        return job

    def get_progress(self, job_id):
        job = self._redis.get(self.job_status_stem + job_id)
        if not job:
            raise NotFound
        job = msgpack.unpackb(job, raw=False)
        if job['progress'] is None:
            return '[queued; not started]'
        if job['progress'] == 0:
            return '[started]'
        if job['progress'] == -1:
            return '[failed]'
        if job['progress'] == 100:
            return '[completed]'
        return str(job['progress'])

    def post(self, msg, callback=None, functions=None):
        msg_id = str(self._redis.incr(self.id_key))
        timestamp = dt.timestamp(dt.utcnow())
        logging.info('Posting message with id {} to {} at {}'.format(msg_id, self.q_name, timestamp))
        pipeline = self._redis.pipeline()
        pipeline.rpush(self.jobs_refs_q, msg_id)
        job = {
            'id': msg_id,
            'timestamp': timestamp,
            'worker': None,
            'processing_started': None,
            'processing_finished': None,
            'progress': None,
            'short_result': None,
            'result': None,
            'callback': callback,
            'retries': 0,
            'functions': functions, # Which function names should be called
            'msg': msg
        }
        job = msgpack.packb(job, use_bin_type=False)
        pipeline.lpush(self.q_name, job)
        pipeline.set(self.job_status_stem + msg_id, job)
        pipeline.execute()
        return msg_id
