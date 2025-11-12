import uuid
from collections.abc import Iterator
from pathlib import Path

import dramatiq
import pytest
from dramatiq.middleware import AgeLimit, Callbacks, Pipelines, Retries, TimeLimit
from pytest_docker import Services

from dramatiq_sqs import SQSBroker


@pytest.fixture(scope="session")
def docker_compose_file() -> str:
    return str(Path(__file__).parent / "compose.yaml")


@pytest.fixture(scope="session")
def elasticmq_endpoint_url(docker_ip: str, docker_services: Services) -> str:
    return "http://{}:{}".format(docker_ip, docker_services.port_for("elasticmq", 9324))


@pytest.fixture
def broker(elasticmq_endpoint_url: str) -> Iterator[SQSBroker]:
    broker = SQSBroker(
        namespace="dramatiq_sqs_tests",
        middleware=[
            AgeLimit(),
            TimeLimit(),
            Callbacks(),
            Pipelines(),
            Retries(min_backoff=1000, max_backoff=900000, max_retries=96),
        ],
        tags={
            "owner": "dramatiq_sqs_tests",
        },
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
def queue_name(broker):
    return f"queue_{uuid.uuid4()}"


@pytest.fixture
def worker(broker):
    worker = dramatiq.Worker(broker)
    worker.start()
    yield worker
    worker.stop()
