import boto3
import dramatiq
import logging
import pytest
import random
import uuid

from dramatiq.middleware import AgeLimit, TimeLimit, Callbacks, Pipelines, Retries
from dramatiq_sqs import SQSBroker

logfmt = "[%(asctime)s] [%(threadName)s] [%(name)s] [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.DEBUG, format=logfmt)
logging.getLogger("botocore").setLevel(logging.WARN)
random.seed(1337)


@pytest.fixture
def broker():
    broker = SQSBroker(
        namespace="dramatiq_sqs_tests",
        middleware=[
            AgeLimit(),
            TimeLimit(),
            Callbacks(),
            Pipelines(),
            Retries(min_backoff=1000, max_backoff=15000, max_retries=120),
        ],
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
