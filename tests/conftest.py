import logging
import random
import uuid

import dramatiq
import pytest
from dramatiq.middleware import AgeLimit, Callbacks, Pipelines, Retries, TimeLimit

from dramatiq_sqs import SQSBroker

logfmt = "[%(asctime)s] [%(threadName)s] [%(name)s] [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.DEBUG, format=logfmt)
logging.getLogger("botocore").setLevel(logging.WARN)
random.seed(1337)


@pytest.fixture
def broker():
    broker = SQSBroker(
        endpoint_url="http://127.0.0.1:9324",
        region_name="elasticmq",
        aws_secret_access_key="x",
        aws_access_key_id="x",
        use_ssl=False,
        namespace="dramatiq_sqs_tests",
        middleware=[
            AgeLimit(),
            TimeLimit(),
            Callbacks(),
            Pipelines(),
            Retries(min_backoff=1, max_backoff=900000, max_retries=2),
        ],
        tags={
            "owner": "dramatiq_sqs_tests",
        },
    )
    dramatiq.set_broker(broker)
    yield broker
    for queue in broker.queues.values():
        queue.delete()


@pytest.fixture
def queue_name(broker):
    return f"queue_{uuid.uuid4()}"


@pytest.fixture
def worker(broker):
    worker = dramatiq.Worker(broker)
    worker.start()
    yield worker
    worker.stop()
