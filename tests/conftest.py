from collections.abc import Generator
from typing import TYPE_CHECKING

import boto3
import dramatiq
import pytest
from dramatiq import Worker
from dramatiq.middleware import AgeLimit, Callbacks, Pipelines, Retries, TimeLimit
from moto.server import ThreadedMotoServer

from dramatiq_sqs import SQSBroker
from dramatiq_sqs.queueset import QueueSet
from dramatiq_sqs.sqs_queue import SQSQueueAttributes

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
def client(sqs_endpoint_url: str) -> "SQSClient":
    return boto3.client(
        "sqs",
        region_name="eu-central-1",
        endpoint_url=sqs_endpoint_url,
        aws_access_key_id="000000000000",
        aws_secret_access_key="000000000000",
    )


@pytest.fixture
def namespace() -> str:
    return "pytest"


@pytest.fixture
def tags() -> dict[str, str]:
    return {"owner": "pytest"}


@pytest.fixture
def dl_queues_enabled() -> bool:
    return False


@pytest.fixture
def queue_attributes() -> SQSQueueAttributes | None:
    return None


@pytest.fixture
def broker(
    client: "SQSClient",
    namespace: str,
    dl_queues_enabled: bool,
    queue_attributes: SQSQueueAttributes,
    tags: dict[str, str],
) -> Generator[SQSBroker]:
    broker = SQSBroker(
        client,
        namespace=namespace,
        middleware=[
            AgeLimit(),
            TimeLimit(),
            Callbacks(),
            Pipelines(),
            Retries(min_backoff=1, max_backoff=10, max_retries=10),
        ],
        dl_queues_enabled=dl_queues_enabled,
        queue_attributes=queue_attributes,
        tags=tags,
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
def queue_name() -> str:
    return "default"


@pytest.fixture
def queueset(broker: SQSBroker, queue_name: str) -> QueueSet:
    broker.declare_queue(queue_name)
    return broker.queuesets[queue_name]


@pytest.fixture
def worker(broker: SQSBroker) -> Generator[Worker]:
    worker = dramatiq.Worker(broker, worker_timeout=10, worker_threads=2)
    worker.start()
    yield worker
    worker.stop()
