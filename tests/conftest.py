import os
os.environ["REDIS_DB"] = "15"   # must be set before core is imported

import pytest
import core

@pytest.fixture(autouse=True)
def clean_redis():
    core.r.flushdb()
    yield
    core.r.flushdb()