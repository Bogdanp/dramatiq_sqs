from typing import TYPE_CHECKING, NamedTuple, TypedDict

import dramatiq

logger = dramatiq.get_logger(__name__)

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient


_SQS_QUEUE_ATTRIBUTES = {
    "MessageRetentionPeriod": str,
}

SQSQueueAttributes = TypedDict("SQSQueueAttributes", _SQS_QUEUE_ATTRIBUTES, total=False)
Tags = dict[str, str]


class SQSQueue(NamedTuple):
    name: str
    url: str
    attributes: SQSQueueAttributes


def _get_sqs_queue_attributes(sqs: "SQSClient", url: str) -> SQSQueueAttributes:
    return sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=list(_SQS_QUEUE_ATTRIBUTES.keys())
    )["Attributes"]


def _create_sqs_queue(
    sqs: "SQSClient", name: str, attributes: SQSQueueAttributes, tags: Tags
) -> SQSQueue:
    url = sqs.create_queue(QueueName=name, Attributes=attributes, tags=tags)["QueueUrl"]
    attributes = _get_sqs_queue_attributes(sqs, url)

    return SQSQueue(name, url, attributes)


def _update_sqs_queue(
    sqs: "SQSClient", queue: SQSQueue, attributes: SQSQueueAttributes, tags: Tags
) -> SQSQueue:
    if tags:
        sqs.tag_queue(QueueUrl=queue.url, Tags=tags)

    attributes = attributes or {}

    if not attributes.items() <= queue.attributes.items():
        sqs.set_queue_attributes(QueueUrl=queue.url, Attributes=attributes)
        queue = SQSQueue(queue.name, queue.url, queue.attributes | attributes)

    return queue


def get_sqs_queue(sqs: "SQSClient", name: str) -> SQSQueue:
    url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
    attributes = _get_sqs_queue_attributes(sqs, url)
    return SQSQueue(name, url, attributes)


def ensure_sqs_queue(
    sqs: "SQSClient",
    name: str,
    attributes: SQSQueueAttributes | None = None,
    tags: Tags | None = None,
) -> SQSQueue:
    attributes = attributes or {}
    tags = tags or {}

    try:
        queue = get_sqs_queue(sqs, name)
        queue = _update_sqs_queue(sqs, queue, attributes, tags)
        return queue

    except sqs.exceptions.QueueDoesNotExist:
        logger.info(f"Queue {name} does not exist, creating")
        return _create_sqs_queue(sqs, name, attributes, tags)
