import asyncio
import pytest

from controller.app import main

def test_import():
    assert main
    
def test_clear_all():
    q = asyncio.run(main('clear_all', 'test_ns', 'localhost'))
    keys = q.redis.keys('test_ns')
    assert isinstance(keys, list)
    assert len(keys) == 0
