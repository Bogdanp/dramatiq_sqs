import dataclasses
import threading
import time
from base64 import b64decode, b64encode
from collections import deque
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import boto3
import dramatiq
from botocore.exceptions import ClientError
from dramatiq.errors import QueueJoinTimeout
from dramatiq.logging import get_logger

from dramatiq_sqs import utils
from dramatiq_sqs.exceptions import MessageDelayTooLong, MessageTooLarge

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import Message, Queue, SQSServiceResource
    from mypy_boto3_sqs.type_defs import (
        ChangeMessageVisibilityBatchRequestEntryTypeDef,
    )

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

#: Default heartbeat configuration. With these defaults, an in-flight message
#: gets its visibility timeout pushed forward by 120s every 30s. If a worker
#: dies (heartbeats stop), the message becomes visible again to other
#: consumers within ~120s — well below the SQS-default 12-hour visibility
#: that would otherwise apply to a stuck message.
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30
DEFAULT_HEARTBEAT_EXTENSION_SECONDS = 120
DEFAULT_HEARTBEAT_MAX_EXTENSIONS = 60


@dataclasses.dataclass
class _HeartbeatState:
    """Per-message heartbeat bookkeeping."""

    message: "_SQSMessage"
    beats_so_far: int = 0


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
      visibility_timeout: The number of seconds SQS will wait for a message to be
        acked before redelivering it. When ``heartbeat_interval`` is set, this
        is the *initial* visibility window — heartbeats push it forward as
        long as the worker is healthy. Defaults to 12 hours.
      heartbeat_interval: How often (in seconds) to extend the visibility
        timeout of in-flight messages via ``ChangeMessageVisibility``. Pass
        ``None`` to disable heartbeats entirely (messages then rely on the
        static ``visibility_timeout`` only). Defaults to 30 seconds.
      heartbeat_extension: How many seconds each heartbeat pushes the
        visibility deadline forward. Must be greater than ``heartbeat_interval``
        so the message can't expire between beats. Defaults to 120 seconds.
      heartbeat_max_extensions: Hard cap on the number of beats per message —
        a runaway guard against a stuck worker pinning a message forever.
        After this many beats, the message is dropped from heartbeat tracking
        and SQS redelivers it to another consumer. Defaults to 60.
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
        heartbeat_interval: int | None = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        heartbeat_extension: int = DEFAULT_HEARTBEAT_EXTENSION_SECONDS,
        heartbeat_max_extensions: int = DEFAULT_HEARTBEAT_MAX_EXTENSIONS,
        tags: dict[str, str] | None = None,
        **options,
    ) -> None:
        self.queue_names: set[str] = set()

        super().__init__(middleware=middleware)

        if (
            retention < MIN_MESSAGE_RETENTION_SECONDS
            or retention > MAX_MESSAGE_RETENTION_SECONDS
        ):
            raise ValueError(
                f"'retention' must be between {MIN_MESSAGE_RETENTION_SECONDS} seconds and "
                f"{MAX_MESSAGE_RETENTION_SECONDS} seconds."
            )

        if heartbeat_interval is not None and heartbeat_extension <= heartbeat_interval:
            raise ValueError(
                f"'heartbeat_extension' ({heartbeat_extension}s) must be greater than "
                f"'heartbeat_interval' ({heartbeat_interval}s); otherwise the SQS "
                f"message expires between beats."
            )

        self.namespace: str | None = namespace
        self.retention = retention
        self.queues: dict[str, Queue] = {}
        self.dead_letter = dead_letter
        self.dead_letter_queues: dict[str, Queue] = {}
        self.dead_letter_retention = dead_letter_retention
        self.visibility_timeout = visibility_timeout
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_extension = heartbeat_extension
        self.heartbeat_max_extensions = heartbeat_max_extensions
        self.tags = tags
        self.sqs: SQSServiceResource = boto3.resource("sqs", **options)

    @property
    def consumer_class(self):
        return SQSConsumer

    def consume(
        self,
        queue_name: str,
        prefetch: int = 1,
        timeout: int = MAX_WAIT_TIME_SECONDS * 1000,
    ) -> dramatiq.Consumer:
        self._ensure_queue(queue_name)

        queue = self.queues[queue_name]
        dead_letter_queue = (
            self.dead_letter_queues[queue_name] if self.dead_letter else None
        )

        return self.consumer_class(
            queue,
            prefetch,
            timeout,
            dead_letter_queue=dead_letter_queue,
            visibility_timeout=self.visibility_timeout,
            heartbeat_interval=self.heartbeat_interval,
            heartbeat_extension=self.heartbeat_extension,
            heartbeat_max_extensions=self.heartbeat_max_extensions,
        )

    def declare_queue(self, queue_name: str) -> None:
        if queue_name not in self.queue_names:
            self.emit_before("declare_queue", queue_name)
            self.queue_names.add(queue_name)
            self.emit_after("declare_queue", queue_name)

    def _ensure_queue(self, queue_name: str) -> None:
        if queue_name not in self.queue_names:
            raise dramatiq.QueueNotFound(queue_name)

        sqs_queue_name = (
            f"{self.namespace}_{queue_name}" if self.namespace else queue_name
        )

        if queue_name not in self.queues:
            self.queues[queue_name] = self._get_or_create_sqs_queue(
                sqs_queue_name, message_retention_period=self.retention, tags=self.tags
            )

        sqs_dead_letter_queue_name = f"{sqs_queue_name}_dlq"

        if self.dead_letter and queue_name not in self.dead_letter_queues:
            self.dead_letter_queues[queue_name] = self._get_or_create_sqs_queue(
                sqs_dead_letter_queue_name,
                message_retention_period=self.dead_letter_retention,
                tags=self.tags,
            )

    def _get_or_create_sqs_queue(
        self,
        sqs_queue_name: str,
        *,
        message_retention_period: int,
        tags: dict[str, str] | None = None,
    ) -> "Queue":
        try:
            queue = self.sqs.get_queue_by_name(QueueName=sqs_queue_name)

            if tags:
                self.sqs.meta.client.tag_queue(QueueUrl=queue.url, Tags=tags)

        except self.sqs.meta.client.exceptions.QueueDoesNotExist:
            self.logger.debug(f"Queue {sqs_queue_name} does not exist, creating")

            queue = self.sqs.create_queue(
                QueueName=sqs_queue_name,
                Attributes={
                    "MessageRetentionPeriod": str(message_retention_period),
                },
                tags=tags or {},
            )

        return queue

    def enqueue(
        self, message: dramatiq.Message, *, delay: int | None = None
    ) -> dramatiq.Message:
        queue_name = message.queue_name
        self._ensure_queue(queue_name)

        queue = self.queues[queue_name]
        delay_seconds = (delay or 0) // 1000

        if delay_seconds > MAX_DELAY_SECONDS:
            raise MessageDelayTooLong(
                f"Messages in SQS cannot be delayed for longer than {MAX_DELAY_SECONDS} seconds."
            )

        encoded_message = b64encode(message.encode()).decode()
        if len(encoded_message) > MAX_MESSAGE_SIZE_BYTES:
            raise MessageTooLarge(
                f"Messages in SQS can be at most {MAX_MESSAGE_SIZE_BYTES} bytes large."
            )

        self.logger.debug(
            "Enqueueing message %r on queue %r.", message.message_id, queue_name
        )
        self.emit_before("enqueue", message, delay)
        queue.send_message(
            MessageBody=encoded_message,
            DelaySeconds=delay_seconds,
        )
        self.emit_after("enqueue", message, delay)
        return message

    def join(self, queue_name: str, *, timeout: int | None = None) -> None:
        queue = self.queues[queue_name]

        deadline = timeout and time.monotonic() + timeout

        while True:
            if deadline and time.monotonic() >= deadline:
                raise QueueJoinTimeout(queue_name)

            queue.load()
            message_count = sum(
                (
                    int(queue.attributes["ApproximateNumberOfMessages"]),
                    int(queue.attributes["ApproximateNumberOfMessagesDelayed"]),
                    int(queue.attributes["ApproximateNumberOfMessagesNotVisible"]),
                )
            )

            if message_count == 0:
                break

            time.sleep(1)

    def get_declared_queues(self) -> Iterable[str]:
        return set(self.queue_names)

    def get_declared_delay_queues(self) -> Iterable[str]:
        return set()


class SQSConsumer(dramatiq.Consumer):
    def __init__(
        self,
        queue: "Queue",
        prefetch: int,
        timeout: int,
        *,
        dead_letter_queue: "Queue",
        visibility_timeout: int | None = None,
        heartbeat_interval: int | None = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        heartbeat_extension: int = DEFAULT_HEARTBEAT_EXTENSION_SECONDS,
        heartbeat_max_extensions: int = DEFAULT_HEARTBEAT_MAX_EXTENSIONS,
    ) -> None:
        self.logger = get_logger(__name__, type(self))
        self.queue = queue
        self.dead_letter_queue = dead_letter_queue
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

        # Heartbeat state. When ``heartbeat_interval`` is None, no ticker
        # thread is started and ``_track``/``_untrack`` are no-ops.
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_extension = heartbeat_extension
        self._heartbeat_max_extensions = heartbeat_max_extensions
        self._tracked: dict[str, _HeartbeatState] = {}
        self._tracked_lock = threading.Lock()
        self._stop = threading.Event()
        self._ticker: threading.Thread | None = None
        if heartbeat_interval is not None:
            self._ticker = threading.Thread(
                target=self._ticker_loop,
                daemon=True,
                name="sqs-heartbeat-ticker",
            )
            self._ticker.start()

    def ack(self, message: "_SQSMessage") -> None:
        message._sqs_message.delete()
        self.message_refc -= 1
        self._untrack(message.message_id)

    def nack(self, message: "_SQSMessage") -> None:
        if self.dead_letter_queue is not None:
            self.dead_letter_queue.send_message(MessageBody=message._sqs_message.body)

        message._sqs_message.delete()
        self.message_refc -= 1
        self._untrack(message.message_id)

    def requeue(self, messages: Iterable["_SQSMessage"]) -> None:
        for batch in utils.batched(messages, 10):
            # Setting the VisibilityTimeout to 0 makes the messages immediately visible again.
            entries: list[ChangeMessageVisibilityBatchRequestEntryTypeDef] = [
                {
                    "Id": str(i),
                    "ReceiptHandle": message._sqs_message.receipt_handle,
                    "VisibilityTimeout": 0,
                }
                for i, message in enumerate(batch)
            ]
            response = self.queue.change_message_visibility_batch(Entries=entries)

            requeued_messages = response.get("Successful", [])
            self.message_refc -= len(requeued_messages)

            for message in batch:
                self._untrack(message.message_id)

    def __next__(self) -> dramatiq.Message | None:
        kw: dict[str, Any] = {
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
                        wrapped = _SQSMessage(sqs_message, dramatiq_message)
                        self.messages.append(wrapped)
                        self.message_refc += 1
                        # Tracking starts at receive (before yield) so prefetched
                        # messages don't expire while waiting for a worker to
                        # pick them up. No-op when heartbeat is disabled.
                        self._track(wrapped)
                    except Exception:  # pragma: no cover
                        self.logger.exception(
                            "Failed to decode message: %r", sqs_message.body
                        )

            try:
                return self.messages.popleft()
            except IndexError:
                return None

    def close(self) -> None:
        if self._ticker is not None:
            self._stop.set()
            # Ticker waits at most one interval before observing the stop event.
            self._ticker.join(timeout=(self._heartbeat_interval or 0) + 1)
        with self._tracked_lock:
            self._tracked.clear()

    def _track(self, message: "_SQSMessage") -> None:
        if self._ticker is None:
            return
        with self._tracked_lock:
            self._tracked[message.message_id] = _HeartbeatState(message=message)

    def _untrack(self, message_id: str) -> None:
        if self._ticker is None:
            return
        with self._tracked_lock:
            self._tracked.pop(message_id, None)

    def _ticker_loop(self) -> None:
        # ``Event.wait(timeout)`` returns True when the event is set, False on
        # timeout. So this loop ticks every ``interval`` seconds and exits
        # promptly on close().
        assert self._heartbeat_interval is not None
        while not self._stop.wait(self._heartbeat_interval):
            self._beat_all()

    def _beat_all(self) -> None:
        # Snapshot under the lock so we don't iterate while another thread
        # mutates. API calls happen without holding the lock.
        with self._tracked_lock:
            entries = list(self._tracked.items())

        # First pass: drop messages past the cap (no API call needed).
        to_beat: list[tuple[str, _HeartbeatState]] = []
        for message_id, state in entries:
            if state.beats_so_far >= self._heartbeat_max_extensions:
                self.logger.error(
                    "Heartbeat cap hit for message %s after %d extensions; "
                    "releasing to SQS.",
                    message_id,
                    self._heartbeat_max_extensions,
                )
                self._untrack(message_id)
                continue
            to_beat.append((message_id, state))

        # Second pass: batch ChangeMessageVisibility (up to 10 per call).
        for batch in utils.batched(to_beat, 10):
            request_entries: list[ChangeMessageVisibilityBatchRequestEntryTypeDef] = [
                {
                    "Id": str(i),
                    "ReceiptHandle": state.message._sqs_message.receipt_handle,
                    "VisibilityTimeout": self._heartbeat_extension,
                }
                for i, (_, state) in enumerate(batch)
            ]
            try:
                response = self.queue.change_message_visibility_batch(
                    Entries=request_entries
                )
            except ClientError as e:
                # Whole-batch failure (network/auth/throttling) — log and let
                # the next tick try again. Don't untrack: the receipt handles
                # are presumably still valid.
                self.logger.warning(
                    "Heartbeat batch failed (%d entries): %s",
                    len(request_entries),
                    e,
                )
                continue

            # Per-entry success: increment beat counter.
            for ok in response.get("Successful", []):
                _, state = batch[int(ok["Id"])]
                state.beats_so_far += 1

            # Per-entry failure: typically ReceiptHandleIsInvalid (message
            # already deleted or redelivered). Log and drop from tracking.
            for fail in response.get("Failed", []):
                message_id, _ = batch[int(fail["Id"])]
                self.logger.warning(
                    "Heartbeat for message %s failed (%s: %s); dropping from tracking.",
                    message_id,
                    fail.get("Code", "?"),
                    fail.get("Message", ""),
                )
                self._untrack(message_id)


class _SQSMessage(dramatiq.MessageProxy):
    def __init__(self, sqs_message: "Message", message: dramatiq.Message) -> None:
        super().__init__(message)

        self._sqs_message = sqs_message
