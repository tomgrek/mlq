import asyncio
import concurrent.futures
from datetime import datetime as dt
import functools
import logging
import time
from uuid import uuid1 as uuid

import msgpack
import redis


class MLQ():
    """Create a queue object"""
    def __init__(self, q_name, redis_host, redis_port, redis_db):
        self.q_name = q_name
        self.processing_q = self.q_name + '_processing'
        self.progress_q = self.q_name + '_progress'
        self.jobs_refs_q = self.q_name + '_jobsrefs'
        # msgs have a 64 bit id starting at 0
        self.id_key = self.q_name + '_max_id'
        self.id = str(uuid())
        self.loop = asyncio.get_running_loop()
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        logging.info('Connected to Redis at {}:{}'.format(redis_host, redis_port))
        self.pool = concurrent.futures.ThreadPoolExecutor()

    def create_listener(self, function):
        """Create a MLQ consumer that executes `function` on message received.
        :param func function: A function with the signature my_func(msg, *args).
        Msg is the posted message, optional args[0] inside the function gives access
        to the entire message object (worker id, timestamp, retries etc)
        If there are multiple consumers, they get messages round-robin
        """
        def listener(function):
            while True:
                msg_str = self.redis.brpoplpush(self.q_name, self.processing_q, timeout=0)
                msg_dict = msgpack.unpackb(msg_str, raw=False)
                # update progress_q with worker id + time started
                msg_dict['worker'] = self.id
                msg_dict['processing_started'] = dt.timestamp(dt.utcnow())
                new_record = msgpack.packb(msg_dict, use_bin_type=False)
                self.redis.set(self.progress_q + '_' + str(msg_dict['id']), new_record)
                # process msg ...
                function(msg_dict['msg'], msg_dict)
                # end processing message
                # remove from progress_q ?? or keep along with result????
                print('completed job!', str(msg_dict['id']))
                self.redis.delete(self.progress_q + '_' + str(msg_dict['id']))
                self.redis.lrem(self.processing_q, -1, msg_str)
                self.redis.lrem(self.jobs_refs_q, 1, str(msg_dict['id']))
        self.loop.run_in_executor(self.pool, functools.partial(listener, function))

    def create_reaper(self, call_how_often=1, job_timeout=30):
        """A thread to reap jobs that were too slow
        :param int call_how_often: How often reaper should be called, every [this] seconds
        :param int job_timeout: Jobs processing for longer than this will be requeued"""
        def reaper():
            while True:
                time.sleep(call_how_often)
                time_now = dt.timestamp(dt.utcnow())
                queued_jobs_length = self.redis.llen(self.jobs_refs_q)
                # check first 5 msgs in queue, if any exceed timeout, keep checking
                for i in range(0, (queued_jobs_length // 5) + 1, 5):
                    job_keys = self.redis.lrange(self.jobs_refs_q, i, i + 5)
                    all_ok = True
                    print(job_keys)
                    for job_key in job_keys:
                        progress_key = self.progress_q + '_' + job_key
                        job_str = self.redis.get(progress_key)
                        if not job_str:
                            logging.warning('Found orphan job {}'.format(job_key))
                            self.redis.lrem(self.jobs_refs_q, 1, job_key)
                            all_ok = False
                            continue
                        job = msgpack.unpackb(job_str, raw=False)
                        if job['worker'] and time_now - job['processing_started'] > job_timeout:
                            logging.warning('Moved job id {} on worker {} back to queue after timeout {}'.format(job['id'], job['worker'], job_timeout))
                            pipeline = self.redis.pipeline()
                            job['processing_started'] = None
                            job['progress'] = None
                            job['worker'] = None
                            pipeline.lrem(self.processing_q, -1, msgpack.packb(job, use_bin_type=False))
                            job['timestamp'] = dt.timestamp(dt.utcnow())
                            job['retries'] += 1
                            # IF RETRIES > MAX_RETRIES ----- MOVE TO DEAD LETTER QUEUE ******
                            job_id = job['id']
                            job = msgpack.packb(job, use_bin_type=False)
                            pipeline.lpush(self.q_name, job)
                            self.redis.lrem(self.jobs_refs_q, 1, job_id)
                            self.redis.rpush(self.jobs_refs_q, job_id)
                            # update the progress queue
                            self.redis.set(self.progress_q + '_' + job_id, job)
                            pipeline.execute()
                            all_ok = False
                            # call callback with failed + requeued status
                            # increment retries and requeue to q_name
                            # delete from processing queue
                    if all_ok:
                        break
        self.loop.run_in_executor(self.pool, reaper)

    def post(self, msg, callback=None):
        msg_id = str(self.redis.incr(self.id_key))
        timestamp = dt.timestamp(dt.utcnow())
        logging.info('Posting message with id {} to queue at {}'.format(msg_id, timestamp))
        pipeline = self.redis.pipeline()
        pipeline.rpush(self.jobs_refs_q, msg_id)
        job = {
            'id': msg_id,
            'timestamp': timestamp,
            'worker': None,
            'processing_started': None,
            'progress': None,
            'callback': callback,
            'retries': 0,
            'msg': msg
        }
        job = msgpack.packb(job, use_bin_type=False)
        pipeline.lpush(self.q_name, job)
        pipeline.set(self.progress_q + '_' + msg_id, job)
        pipeline.execute()
        return msg_id
