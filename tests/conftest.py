import uuid
from collections.abc import Generator
from typing import TYPE_CHECKING

import dramatiq
import pytest
from dramatiq import Worker
from dramatiq.middleware import AgeLimit, Callbacks, Pipelines, Retries, TimeLimit
from moto.server import ThreadedMotoServer
from mypy_boto3_sqs import SQSClient

from dramatiq_sqs import SQSBroker
from dramatiq_sqs.queueset import QueueSet

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient


@pytest.fixture(scope="session")
def moto_server() -> Generator[ThreadedMotoServer]:
    server = ThreadedMotoServer(port=0)
    server.start()

    yield server

    server.stop()


@pytest.fixture(scope="session")
def sqs_endpoint_url(moto_server: ThreadedMotoServer) -> str:
    host, port = moto_server.get_host_and_port()
    return f"http://{host}:{port}"


@pytest.fixture
def namespace() -> str:
    return "pytest"


@pytest.fixture
def tags() -> dict[str, str]:
    return {"owner": "pytest"}


@pytest.fixture
def dead_letter() -> bool:
    return False


@pytest.fixture
def max_message_size_bytes() -> int | None:
    return None


@pytest.fixture
def broker(
    sqs_endpoint_url: str,
    namespace: str,
    dead_letter: bool,
    tags: dict[str, str],
    max_message_size_bytes: int | None,
) -> Generator[SQSBroker]:
    broker = SQSBroker(
        namespace=namespace,
        middleware=[
            AgeLimit(),
            TimeLimit(),
            Callbacks(),
            Pipelines(),
            Retries(min_backoff=1000, max_backoff=900000, max_retries=96),
        ],
        dead_letter=dead_letter,
        max_message_size=max_message_size_bytes,
        tags=tags,
        region_name="eu-central-1",
        endpoint_url=sqs_endpoint_url,
        aws_access_key_id="000000000000",
        aws_secret_access_key="000000000000",
    )

    dramatiq.set_broker(broker)

    yield broker

    for queueset in broker.queuesets.values():
        broker.client.delete_queue(QueueUrl=queueset.queue.url)

        if queueset.dl_queue is not None:
            broker.client.delete_queue(QueueUrl=queueset.dl_queue.url)


@pytest.fixture
def sqs(broker: SQSBroker) -> "SQSClient":
    return broker.client


@pytest.fixture
def queue_name(broker: SQSBroker) -> str:
    return f"queue_{uuid.uuid4()}"


@pytest.fixture
def queueset(broker: SQSBroker, queue_name: str) -> QueueSet:
    broker.declare_queue(queue_name)
    return broker.queuesets[queue_name]


@pytest.fixture
def worker(broker: SQSBroker) -> Generator[Worker]:
    worker = dramatiq.Worker(broker)
    worker.start()
    yield worker
    worker.stop()
