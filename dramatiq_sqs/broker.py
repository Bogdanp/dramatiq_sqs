import json
import os
from base64 import b64decode, b64encode
from collections import deque
from typing import Any, Dict, Iterable, List, Optional, Sequence, TypeVar

import boto3
import dramatiq
from dramatiq.logging import get_logger

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

#: The number of times a message will be received before being added
#: to the dead-letter queue (if enabled).
MAX_RECEIVES = 5


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
      retention: The number of seconds messages can be retained for.
        Defaults to 14 days.
      dead_letter: Whether to add a dead-letter queue. Defaults to false.
      max_receives: The number of times a message should be received before
        being added to the dead-letter queue. Defaults to MAX_RECEIVES.
      **options: Additional options that are passed to boto3.

    .. _Dramatiq: https://dramatiq.io
    .. _Amazon SQS: https://aws.amazon.com/sqs/
    .. _boto3: http://boto3.readthedocs.io/en/latest/index.html
    .. _docs: http://boto3.readthedocs.io/en/latest/guide/configuration.html
    """

    def __init__(
            self, *,
            namespace: Optional[str] = None,
            middleware: Optional[List[dramatiq.Middleware]] = None,
            retention: int = MAX_MESSAGE_RETENTION_SECONDS,
            dead_letter: bool = False,
            max_receives: int = MAX_RECEIVES,
            visibility_timeout: Optional[int] = MAX_VISIBILITY_TIMEOUT_SECONDS,
            tags: Optional[Dict[str, str]] = None,
            **options,
    ) -> None:
        super().__init__(middleware=middleware)

        if retention < MIN_MESSAGE_RETENTION_SECONDS or retention > MAX_MESSAGE_RETENTION_SECONDS:
            raise ValueError(
                f"'retention' must be between {MIN_MESSAGE_RETENTION_SECONDS} seconds and "
                f"{MAX_MESSAGE_RETENTION_SECONDS} seconds."
            )

        self.namespace: Optional[str] = namespace
        self.retention: str = str(retention)
        self.queues: Dict[str, Any] = {}
        self.dead_letter: bool = dead_letter
        self.max_receives: int = max_receives
        self.visibility_timeout = visibility_timeout
        self.tags: Optional[Dict[str, str]] = tags
        self.sqs: Any = boto3.resource("sqs", **options)

    @property
    def consumer_class(self):
        return SQSConsumer

    def consume(self, queue_name: str, prefetch: int = 1, timeout: int = 30000) -> dramatiq.Consumer:
        try:
            return self.consumer_class(
                self.queues[queue_name],
                prefetch,
                timeout,
                visibility_timeout=self.visibility_timeout
            )
        except KeyError:  # pragma: no cover
            raise dramatiq.QueueNotFound(queue_name)

    def declare_queue(self, queue_name: str) -> None:
        if queue_name not in self.queues:
            prefixed_queue_name = queue_name
            if self.namespace is not None:
                prefixed_queue_name = "%(namespace)s_%(queue_name)s" % {
                    "namespace": self.namespace,
                    "queue_name": queue_name,
                }

            self.emit_before("declare_queue", queue_name)

            self.queues[queue_name] = self._get_or_create_queue(
                QueueName=prefixed_queue_name,
                Attributes={
                    "MessageRetentionPeriod": self.retention,
                }
            )
            if self.tags:
                self.sqs.meta.client.tag_queue(
                    QueueUrl=self.queues[queue_name].url,
                    Tags=self.tags
                )

            if self.dead_letter:
                dead_letter_queue_name = f"{prefixed_queue_name}_dlq"
                dead_letter_queue = self._get_or_create_queue(
                    QueueName=dead_letter_queue_name
                )
                if self.tags:
                    self.sqs.meta.client.tag_queue(
                        QueueUrl=dead_letter_queue.url,
                        Tags=self.tags
                    )
                redrive_policy = {
                    "deadLetterTargetArn": dead_letter_queue.attributes["QueueArn"],
                    "maxReceiveCount": str(self.max_receives)
                }
                self.queues[queue_name].set_attributes(Attributes={
                    "RedrivePolicy": json.dumps(redrive_policy)
                })
            self.emit_after("declare_queue", queue_name)

    def _get_or_create_queue(self, **kwargs) -> Any:
        try:
            return self.sqs.get_queue_by_name(QueueName=kwargs['QueueName'])
        except self.sqs.meta.client.exceptions.QueueDoesNotExist:
            self.logger.debug(f'Queue does not exist, creating queue with params: {kwargs}')
            return self.sqs.create_queue(**kwargs)

    def enqueue(self, message: dramatiq.Message, *, delay: Optional[int] = None) -> dramatiq.Message:
        queue_name = message.queue_name
        queue = self.queues[queue_name]
        delay_seconds = (delay or 0) // 1000

        if delay_seconds > MAX_DELAY_SECONDS:
            raise ValueError(
                f"Messages in SQS cannot be delayed for longer than {MAX_DELAY_SECONDS} seconds."
            )

        encoded_message = b64encode(message.encode()).decode()
        if len(encoded_message) > MAX_MESSAGE_SIZE_BYTES:
            raise RuntimeError("Messages in SQS can be at most {MAX_MESSAGE_SIZE_BYTES} bytes large.")

        self.logger.debug("Enqueueing message %r on queue %r.", message.message_id, queue_name)
        self.emit_before("enqueue", message, delay)
        queue.send_message(
            MessageBody=encoded_message,
            DelaySeconds=delay_seconds,
        )
        self.emit_after("enqueue", message, delay)
        return message

    def get_declared_queues(self) -> Iterable[str]:
        return set(self.queues)

    def get_declared_delay_queues(self) -> Iterable[str]:
        return set()


class SQSConsumer(dramatiq.Consumer):
    def __init__(
        self, 
        queue: Any, 
        prefetch: int, 
        timeout: int, *, 
        visibility_timeout: Optional[int] = None,
    ) -> None:
        self.logger = get_logger(__name__, type(self))
        self.queue = queue
        self.prefetch = min(prefetch, MAX_PREFETCH)
        
        self.visibility_timeout = visibility_timeout

        if self.visibility_timeout is not None and self.visibility_timeout > MAX_VISIBILITY_TIMEOUT_SECONDS:
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

    def ack(self, message: "_SQSMessage") -> None:
        message._sqs_message.delete()
        self.message_refc -= 1

    #: Messages are added to DLQ by SQS redrive policy, so no actions are necessary
    nack = ack

    def requeue(self, messages: Iterable["_SQSMessage"]) -> None:
        for batch in chunk(messages, chunksize=10):
            # Setting the VisibilityTimeout to 0 makes the messages immediately visible again.
            response = self.queue.change_message_visibility_batch(Entries=[{
                "Id": str(i),
                "ReceiptHandle": message._sqs_message.receipt_handle,
                "VisibilityTimeout": 0,
            } for i, message in enumerate(batch)])

            requeued_messages = response.get("Successful", [])
            self.message_refc -= len(requeued_messages)

    def __next__(self) -> Optional[dramatiq.Message]:
        kw = {
            "MaxNumberOfMessages": self.prefetch,
            "WaitTimeSeconds": self.wait_time_seconds,
        }
        if self.visibility_timeout is not None:
            kw["VisibilityTimeout"] = self.visibility_timeout

        try:
            return self.messages.popleft()
        except IndexError:
            if self.message_refc < self.prefetch:
                for sqs_message in self.queue.receive_messages(**kw):
                    try:
                        encoded_message = b64decode(sqs_message.body)
                        dramatiq_message = dramatiq.Message.decode(encoded_message)
                        self.messages.append(_SQSMessage(sqs_message, dramatiq_message))
                        self.message_refc += 1
                    except Exception:  # pragma: no cover
                        self.logger.exception("Failed to decode message: %r", sqs_message.body)

            try:
                return self.messages.popleft()
            except IndexError:
                return None


class _SQSMessage(dramatiq.MessageProxy):
    def __init__(self, sqs_message: Any, message: dramatiq.Message) -> None:
        super().__init__(message)

        self._sqs_message = sqs_message


T = TypeVar("T")


def chunk(xs: Iterable[T], *, chunksize=10) -> Iterable[Sequence[T]]:
    """Split a sequence into subseqs of chunksize length.
    """
    chunk = []
    for x in xs:
        chunk.append(x)
        if len(chunk) == chunksize:
            yield chunk
            chunk = []

    if chunk:
        yield chunk
