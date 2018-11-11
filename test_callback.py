import argparse
import asyncio
import functools
import logging
from threading import Thread
import time

from flask import Flask, request

app = Flask(__name__)
@app.route('/callbackurl')
def a_test_server():
    print(request.args.get('short_result', None))
    return 'ok'
app.run()
