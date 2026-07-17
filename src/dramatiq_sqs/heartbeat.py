import dataclasses
import threading
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError
from dramatiq.logging import get_logger

from dramatiq_sqs import utils

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import Queue

    from .broker import _SQSMessage

#: Defaults used once the heartbeat is enabled (see ``SQSBroker.heartbeat_interval``).
DEFAULT_HEARTBEAT_EXTENSION_SECONDS = 120
DEFAULT_HEARTBEAT_MAX_EXTENSIONS = 60

#: Per-message errors that mean the receipt handle no longer refers to an
#: in-flight message (already deleted, redelivered or expired). We stop tracking
#: on these. Any other failure is treated as transient and retried next beat.
_STALE_RECEIPT_ERROR_CODES = frozenset({"ReceiptHandleIsInvalid", "MessageNotInflight"})


@dataclasses.dataclass
class _Tracked:
    message: "_SQSMessage"
    beats: int = 0


def validate_config(interval: int | None, extension: int) -> None:
    if interval is not None and extension <= interval:
        raise ValueError(
            f"'heartbeat_extension' ({extension}s) must be greater than "
            f"'heartbeat_interval' ({interval}s), otherwise the message could "
            f"expire between beats."
        )


class Heartbeat:
    """Keeps in-flight SQS messages invisible to other consumers by
    periodically extending their visibility timeout on a background thread.

    A message is tracked from the moment it's received until it's acked, nacked
    or requeued, so both long-running tasks and messages sitting in the
    consumer's prefetch buffer are covered.

    When ``interval`` is ``None`` the heartbeat is disabled: no thread runs and
    :meth:`track`/:meth:`untrack` are no-ops.

    Each beat extends all tracked messages in one ``ChangeMessageVisibilityBatch``
    call (in batches of 10), so it costs one request per beat rather than one
    per message.

    Beats run on one background thread per consumer. Because of the GIL, a
    worker running CPU-bound work that never releases it can delay beats, so
    leave enough headroom between ``interval`` and ``extension`` to absorb that
    (the default 30s/120s leaves 90s of slack). The batch call is I/O and
    releases the GIL, so the beat thread itself does not block workers.

    Parameters:
      queue: The SQS queue whose messages this heartbeat extends.
      interval: How often (in seconds) to extend visibility, or ``None`` to
        disable.
      extension: Seconds each beat pushes the visibility deadline forward. Must
        be greater than ``interval``.
      max_extensions: Cap on beats per message. After this many beats the
        message is dropped from tracking and left to SQS to redeliver.
    """

    def __init__(
        self,
        queue: "Queue",
        *,
        interval: int | None,
        extension: int = DEFAULT_HEARTBEAT_EXTENSION_SECONDS,
        max_extensions: int = DEFAULT_HEARTBEAT_MAX_EXTENSIONS,
    ) -> None:
        validate_config(interval, extension)

        self.logger = get_logger(__name__, type(self))
        self.queue = queue
        self.interval = interval
        self.extension = extension
        self.max_extensions = max_extensions
        self._tracked: dict[str, _Tracked] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._ticker: threading.Thread | None = None

    @property
    def enabled(self) -> bool:
        return self.interval is not None

    def start(self) -> None:
        if not self.enabled:
            return
        self._ticker = threading.Thread(
            target=self._loop,
            daemon=True,
            name="sqs-heartbeat",
        )
        self._ticker.start()

    def stop(self) -> None:
        if self._ticker is not None:
            self._stop.set()
            self._ticker.join(timeout=(self.interval or 0) + 1)
        with self._lock:
            self._tracked.clear()

    def track(self, message: "_SQSMessage") -> None:
        if not self.enabled:
            return
        with self._lock:
            self._tracked[message.message_id] = _Tracked(message)

    def untrack(self, message_id: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._tracked.pop(message_id, None)

    def _loop(self) -> None:
        # wait() returns True when stop is set and False on timeout, so this
        # beats every interval and exits promptly on stop().
        assert self.interval is not None
        while not self._stop.wait(self.interval):
            self._beat()

    def _beat(self) -> None:
        # Snapshot under the lock, then call the API without holding it so
        # track/untrack aren't blocked on the network.
        with self._lock:
            snapshot = list(self._tracked.items())

        to_beat = []
        for message_id, state in snapshot:
            if state.beats >= self.max_extensions:
                self.logger.warning(
                    "Heartbeat cap hit for message %s after %d extensions, "
                    "releasing it to SQS.",
                    message_id,
                    self.max_extensions,
                )
                self.untrack(message_id)
            else:
                to_beat.append((message_id, state))

        for batch in utils.batched(to_beat, 10):
            self._extend(batch)

    def _extend(self, batch: tuple[tuple[str, "_Tracked"], ...]) -> None:
        entries = [
            {
                "Id": str(i),
                "ReceiptHandle": state.message._sqs_message.receipt_handle,
                "VisibilityTimeout": self.extension,
            }
            for i, (_, state) in enumerate(batch)
        ]
        client = self.queue.meta.client
        try:
            response = self.queue.change_message_visibility_batch(Entries=entries)
        except client.exceptions.QueueDoesNotExist:
            # No queue means no messages to keep alive.
            self.logger.warning(
                "Heartbeat queue no longer exists, dropping %d messages from tracking.",
                len(batch),
            )
            for message_id, _ in batch:
                self.untrack(message_id)
            return
        except ClientError as e:
            # Transient failure (throttling, auth, network). Keep everything
            # tracked and retry next beat.
            self.logger.warning(
                "Heartbeat batch of %d failed (%s), retrying next beat.",
                len(entries),
                e.response.get("Error", {}).get("Code", "") or e,
            )
            return

        for ok in response.get("Successful", []):
            _, state = batch[int(ok["Id"])]
            state.beats += 1

        for fail in response.get("Failed", []):
            message_id, _ = batch[int(fail["Id"])]
            if fail.get("Code") in _STALE_RECEIPT_ERROR_CODES:
                # Message is already gone, nothing to keep alive.
                self.untrack(message_id)
            else:
                self.logger.warning(
                    "Heartbeat for message %s failed (%s), retrying next beat.",
                    message_id,
                    fail.get("Code", ""),
                )
