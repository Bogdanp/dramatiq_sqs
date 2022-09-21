import json
from typing import Any, Dict, Iterable, Optional, Sequence, TypeVar

from dramatiq_sqs.dto import QueueName

T = TypeVar("T")


def chunk(xs: Iterable[T], *, chunksize=10) -> Iterable[Sequence[T]]:
    """Split a sequence into subseqs of chunksize length."""
    chunk = []
    for x in xs:
        chunk.append(x)
        if len(chunk) == chunksize:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


def build_queue_name(queue_name: str, namespace: Optional[str] = None) -> QueueName:
    if namespace:
        queue_name = f"{namespace}_{queue_name}"

    return QueueName(default=queue_name, dlq=f"{queue_name}_dlq")


def get_queue(sqs: Any, queue_name: str) -> Any:
    return sqs.get_queue_by_name(QueueName=queue_name)


def create_queue(
    sqs: Any,
    queue_names: QueueName,
    max_receives: int,
    retention: int,
    tags: Optional[Dict[str, str]],
    dead_letter: bool,
) -> Any:
    queue = sqs.create_queue(
        QueueName=queue_names.default,
        Attributes={
            "MessageRetentionPeriod": str(retention),
        },
    )

    if tags:
        sqs.meta.client.tag_queue(QueueUrl=queue.url, Tags=tags)

    if dead_letter:
        dead_letter_queue = sqs.create_queue(QueueName=queue_names.dlq)

        if tags:
            sqs.meta.client.tag_queue(QueueUrl=dead_letter_queue.url, Tags=tags)

        redrive_policy = {
            "deadLetterTargetArn": dead_letter_queue.attributes["QueueArn"],
            "maxReceiveCount": str(max_receives),
        }
        queue.set_attributes(Attributes={"RedrivePolicy": json.dumps(redrive_policy)})

    return queue
