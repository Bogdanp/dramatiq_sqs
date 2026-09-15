import time
from base64 import b64decode, b64encode
from collections import deque
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import boto3
import dramatiq
from dramatiq.common import compute_backoff
from dramatiq.errors import QueueJoinTimeout
from dramatiq.logging import get_logger

from dramatiq_sqs import utils
from dramatiq_sqs.exceptions import MessageDelayTooLong, MessageTooLarge
from dramatiq_sqs.queueset import QueueSet, QueueSetFactory, QueueSetRegistry
from dramatiq_sqs.sqs_queue import SQSQueue, Tags, ensure_sqs_queue

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient
    from mypy_boto3_sqs.type_defs import MessageTypeDef

# SQS quotas:
# https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html

#: The max number of bytes in a message.
MAX_MESSAGE_SIZE_BYTES = 1024 * 1024

#: The min and max number of seconds messages may be retained for.
MIN_MESSAGE_RETENTION_SECONDS = 60
MAX_MESSAGE_RETENTION_SECONDS = 14 * 86400

#: The maximum number of seconds SQS will wait for the message to be acked before
#: redelivering it.
MAX_VISIBILITY_TIMEOUT_SECONDS = 12 * 3600

#: The maximum number of seconds a message can be delayed for.
MAX_DELAY_SECONDS = 15 * 60

#: The max number of messages that may be prefetched at a time.
MAX_PREFETCH = 10

#: The max value for WaitTimeSeconds.
#: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
MAX_WAIT_TIME_SECONDS = 20


def SQSQueueSetFactory(
    sqs: "SQSClient",
    namespace: str | None = None,
    *,
    queue_retention: int,
    dl_queues_enabled: bool = False,
    dl_queue_retention: int | None = None,
    tags: Tags | None = None,
) -> QueueSetFactory[SQSQueue]:
    def factory(name: str) -> QueueSet[SQSQueue]:
        queue_name = "_".join(filter(None, (namespace, name)))
        queue_attributes = {
            "MessageRetentionPeriod": str(queue_retention),
        }
        queue = ensure_sqs_queue(sqs, queue_name, queue_attributes, tags)

        if dl_queues_enabled:
            dl_queue_name = "_".join(filter(None, (namespace, name, "dlq")))
            dl_queue_attributes = {
                "MessageRetentionPeriod": str(dl_queue_retention or queue_retention),
            }
            dl_queue = ensure_sqs_queue(sqs, dl_queue_name, dl_queue_attributes, tags)
        else:
            dl_queue = None

        return QueueSet[SQSQueue](name, queue, dl_queue)

    return factory


class SQSBroker(dramatiq.Broker):
    """A Dramatiq_ broker that can be used with `Amazon SQS`_

    This backend has a number of limitations compared to the built-in
    Redis and RMQ backends:

      * the max amount of time messages can be delayed by is 15 minutes,
      * messages can be at most 1MiB large and
      * messages must be processed within 12 hours of being pulled,
      otherwise they will be redelivered.

    The backend uses boto3_ under the hood.  For details on how
    authorization works, check out its docs_.

    Parameters:
      namespace: The prefix to use when creating queues.
      middleware: The set of middleware that apply to this broker.
      retention: The number of seconds messages will be retained for in the queue.
        Defaults to 14 days.
      dead_letter: Whether to add a dead-letter queue. Defaults to false.
      dead_letter_retention: The number of seconds messages will be retained for in the
        dead letter queue (if enabled). Defaults to 14 days.
      max_message_size: The maximum size (in bytes) of a base64-encoded message.
        Messages larger than this raise :class:`MessageTooLarge` on enqueue.
        Defaults to 1MiB (the SQS maximum), but may be raised for SQS-compatible
        backends that support larger messages.
      **options: Additional options that are passed to boto3.

    .. _Dramatiq: https://dramatiq.io
    .. _Amazon SQS: https://aws.amazon.com/sqs/
    .. _boto3: http://boto3.readthedocs.io/en/latest/index.html
    .. _docs: http://boto3.readthedocs.io/en/latest/guide/configuration.html
    """

    def __init__(
        self,
        *,
        namespace: str | None = None,
        middleware: list[dramatiq.Middleware] | None = None,
        retention: int = MAX_MESSAGE_RETENTION_SECONDS,
        dead_letter: bool = False,
        dead_letter_retention: int = MAX_MESSAGE_RETENTION_SECONDS,
        visibility_timeout: int | None = MAX_VISIBILITY_TIMEOUT_SECONDS,
        max_message_size: int = MAX_MESSAGE_SIZE_BYTES,
        tags: dict[str, str] | None = None,
        **options,
    ) -> None:
        if (
            retention < MIN_MESSAGE_RETENTION_SECONDS
            or retention > MAX_MESSAGE_RETENTION_SECONDS
        ):
            raise ValueError(
                f"'retention' must be between {MIN_MESSAGE_RETENTION_SECONDS} seconds and "
                f"{MAX_MESSAGE_RETENTION_SECONDS} seconds."
            )

        if max_message_size < 1:
            raise ValueError("'max_message_size' must be a positive number of bytes.")

        self.client = boto3.client("sqs", **options)
        self.queuesets = QueueSetRegistry[SQSQueue](
            factory=SQSQueueSetFactory(
                self.client,
                namespace,
                queue_retention=retention,
                dl_queues_enabled=dead_letter,
                dl_queue_retention=dead_letter_retention,
                tags=tags,
            )
        )
        super().__init__(middleware=middleware)
        self.max_message_size = max_message_size
        self.visibility_timeout = visibility_timeout

    @property
    def consumer_class(self):
        return SQSConsumer

    def consume(
        self,
        queue_name: str,
        prefetch: int = 1,
        timeout: int = MAX_WAIT_TIME_SECONDS * 1000,
    ) -> dramatiq.Consumer:
        return self.consumer_class(
            self.client,
            self.queuesets[queue_name].queue,
            prefetch,
            timeout,
            dl_queue=self.queuesets[queue_name].dl_queue,
            visibility_timeout=self.visibility_timeout,
        )

    def declare_queue(self, queue_name: str) -> None:
        if queue_name not in self.queuesets:
            self.emit_before("declare_queue", queue_name)
            self.queuesets.declare_queueset(queue_name)
            self.emit_after("declare_queue", queue_name)

    def enqueue(
        self, message: dramatiq.Message, *, delay: int | None = None
    ) -> dramatiq.Message:
        queueset = self.queuesets[message.queue_name]
        queue = queueset.queue
        delay_seconds = (delay or 0) // 1000

        if delay_seconds > MAX_DELAY_SECONDS:
            raise MessageDelayTooLong(
                f"Messages in SQS cannot be delayed for longer than {MAX_DELAY_SECONDS} seconds."
            )

        encoded_message = b64encode(message.encode()).decode()
        if len(encoded_message) > self.max_message_size:
            raise MessageTooLarge(
                f"Messages in SQS can be at most {self.max_message_size} bytes large."
            )

        self.logger.debug(
            "Enqueueing message %r on queue %r.", message.message_id, queue.name
        )
        self.emit_before("enqueue", message, delay)
        self.client.send_message(
            QueueUrl=queue.url,
            MessageBody=encoded_message,
            DelaySeconds=delay_seconds,
        )
        self.emit_after("enqueue", message, delay)
        return message

    def join(self, queue_name: str, *, timeout: int | None = None) -> None:
        queue = self.queuesets[queue_name].queue

        deadline = timeout and time.monotonic() + timeout

        while True:
            if deadline and time.monotonic() >= deadline:
                raise QueueJoinTimeout(queue_name)

            attributes = self.client.get_queue_attributes(
                QueueUrl=queue.url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesDelayed",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )["Attributes"]
            message_count = sum(
                (
                    int(attributes["ApproximateNumberOfMessages"]),
                    int(attributes["ApproximateNumberOfMessagesDelayed"]),
                    int(attributes["ApproximateNumberOfMessagesNotVisible"]),
                )
            )

            if message_count == 0:
                break

            time.sleep(1)

    def get_declared_queues(self) -> Iterable[str]:
        return self.queuesets.declared_queuesets

    def get_declared_delay_queues(self) -> Iterable[str]:
        return set()


class SQSConsumer(dramatiq.Consumer):
    def __init__(
        self,
        client: "SQSClient",
        queue: SQSQueue,
        prefetch: int,
        timeout: int,
        *,
        dl_queue: SQSQueue | None,
        visibility_timeout: int | None = None,
    ) -> None:
        self.logger = get_logger(__name__, type(self))
        self.client = client
        self.queue = queue
        self.dl_queue = dl_queue
        self.prefetch = min(prefetch, MAX_PREFETCH)

        self.visibility_timeout = visibility_timeout

        if (
            self.visibility_timeout is not None
            and self.visibility_timeout > MAX_VISIBILITY_TIMEOUT_SECONDS
        ):
            raise ValueError(
                f"The message visibility timeout of {self.visibility_timeout} is higher than "
                f"the maximum supported ({MAX_VISIBILITY_TIMEOUT_SECONDS})."
            )

        self.wait_time_seconds = timeout // 1000

        if self.wait_time_seconds > MAX_WAIT_TIME_SECONDS:
            raise ValueError(
                f"The consumer timeout of {self.wait_time_seconds} is higher than "
                f"the maximum supported ({MAX_WAIT_TIME_SECONDS})."
            )

        self.messages: deque = deque()
        self.message_refc = 0
        self.misses = 0

    def ack(self, message: "_SQSMessage") -> None:
        self.client.delete_message(
            QueueUrl=self.queue.url,
            ReceiptHandle=message._sqs_message["ReceiptHandle"],
        )
        self.message_refc -= 1

    def nack(self, message: "_SQSMessage") -> None:
        if self.dl_queue is not None:
            self.client.send_message(
                QueueUrl=self.dl_queue.url,
                MessageBody=message._sqs_message["Body"],
            )

        self.client.delete_message(
            QueueUrl=self.queue.url,
            ReceiptHandle=message._sqs_message["ReceiptHandle"],
        )
        self.message_refc -= 1

    def requeue(self, messages: Iterable["_SQSMessage"]) -> None:
        for batch in utils.batched(messages, 10):
            # Setting the VisibilityTimeout to 0 makes the messages immediately visible again.
            response = self.client.change_message_visibility_batch(
                QueueUrl=self.queue.url,
                Entries=[
                    {
                        "Id": str(i),
                        "ReceiptHandle": message._sqs_message["ReceiptHandle"],
                        "VisibilityTimeout": 0,
                    }
                    for i, message in enumerate(batch)
                ],
            )

            requeued_messages = response.get("Successful", [])
            self.message_refc -= len(requeued_messages)

    def close(self) -> None:
        # Drain the prefetch buffer: messages fetched from SQS but never
        # returned via ``__next__`` are invisible until ``VisibilityTimeout``
        # expires. ``Worker.stop`` drains ``work_queue`` and ``delay_queue``
        # but has no awareness of this internal buffer, so ``close`` is the
        # last hook to put them back.
        buffered, self.messages = list(self.messages), deque()
        if buffered:
            self.requeue(buffered)

    def __next__(self) -> dramatiq.Message | None:
        kw: dict[str, Any] = {
            "MaxNumberOfMessages": self.prefetch,
            "WaitTimeSeconds": self.wait_time_seconds,
        }
        if self.visibility_timeout is not None:
            kw["VisibilityTimeout"] = self.visibility_timeout

        try:
            message = self.messages.popleft()
            self.misses = 0
            return message
        except IndexError:
            if self.message_refc < self.prefetch:
                response = self.client.receive_message(QueueUrl=self.queue.url, **kw)
                for sqs_message in response.get("Messages", []):
                    try:
                        encoded_message = b64decode(sqs_message["Body"])
                        dramatiq_message = dramatiq.Message.decode(encoded_message)
                        self.messages.append(_SQSMessage(sqs_message, dramatiq_message))
                        self.message_refc += 1
                    except Exception:  # pragma: no cover
                        self.logger.exception(
                            "Failed to decode message: %r", sqs_message["Body"]
                        )

            try:
                message = self.messages.popleft()
                self.misses = 0
                return message
            except IndexError:
                # Back off to avoid spinning when the prefetch limit is
                # reached or no messages are available. SQS long-polling
                # already throttles fetches, but when message_refc >=
                # prefetch we don't fetch at all and would otherwise busy
                # loop until a worker thread frees a slot.
                self.misses, backoff_ms = compute_backoff(
                    self.misses, max_backoff=self.wait_time_seconds * 1000 or 1000
                )
                time.sleep(backoff_ms / 1000)
                return None


class _SQSMessage(dramatiq.MessageProxy):
    def __init__(
        self, sqs_message: "MessageTypeDef", message: dramatiq.Message
    ) -> None:
        super().__init__(message)

        self._sqs_message = sqs_message
