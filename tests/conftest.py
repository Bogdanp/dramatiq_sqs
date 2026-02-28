import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import dramatiq
import pytest
from dramatiq.middleware import AgeLimit, Callbacks, Pipelines, Retries, TimeLimit
from mypy_boto3_sqs import SQSServiceResource
from pytest_docker import Services

from dramatiq_sqs import SQSBroker

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSServiceResource


@pytest.fixture(scope="session")
def docker_compose_file() -> str:
    return str(Path(__file__).parent / "compose.yaml")


@pytest.fixture(scope="session")
def elasticmq_endpoint_url(docker_ip: str, docker_services: Services) -> str:
    return "http://{}:{}".format(docker_ip, docker_services.port_for("elasticmq", 9324))


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
def broker(
    elasticmq_endpoint_url: str, namespace: str, dead_letter: bool, tags: dict[str, str]
) -> Iterator[SQSBroker]:
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
        tags=tags,
        region_name="eu-central-1",
        endpoint_url=elasticmq_endpoint_url,
        aws_access_key_id="000000000000",
        aws_secret_access_key="000000000000",
    )
    dramatiq.set_broker(broker)

    yield broker

    for queue in broker.queues.values():
        queue.delete()


@pytest.fixture
def sqs(broker: SQSBroker) -> "SQSServiceResource":
    return broker.sqs


@pytest.fixture
def queue_name(broker):
    return f"queue_{uuid.uuid4()}"


@pytest.fixture
def worker(broker):
    worker = dramatiq.Worker(broker)
    worker.start()
    yield worker
    worker.stop()
