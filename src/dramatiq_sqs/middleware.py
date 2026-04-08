import traceback

from dramatiq.common import compute_backoff
from dramatiq.errors import Retry
from dramatiq.logging import get_logger
from dramatiq.middleware import Middleware

from .broker import MAX_VISIBILITY_TIMEOUT_SECONDS

#: The default minimum amount of backoff to apply to retried tasks (in milliseconds).
DEFAULT_MIN_BACKOFF = 15000

#: The default maximum amount of backoff to apply to retried tasks (in milliseconds).
DEFAULT_MAX_BACKOFF = 86400000 * 7


class SQSRetries(Middleware):
    """Middleware that automatically retries failed tasks using SQS native
    message redelivery instead of re-enqueueing new messages.

    This middleware should be used **instead of** the core
    :class:`Retries<dramatiq.middleware.Retries>` middleware when using
    :class:`SQSBroker<dramatiq_sqs.SQSBroker>`.

    Unlike the core ``Retries`` middleware which deletes the original message
    and enqueues a new one, this middleware keeps the same SQS message and
    uses visibility timeout to control backoff. This allows SQS's
    ``ApproximateReceiveCount`` to increment correctly, enabling:

    * **Dead-letter queue routing** via SQS redrive policies
    * **Native retry tracking** via ``ApproximateReceiveCount``

    When ``dead_letter=True`` on the broker, retry limits are controlled
    entirely by the SQS redrive policy's ``maxReceiveCount``. The
    ``max_retries`` parameter is not enforced by the middleware.

    When ``dead_letter=False``, the middleware enforces ``max_retries``
    using ``ApproximateReceiveCount`` and deletes messages that exceed the
    limit.

    Parameters:
      max_retries: The maximum number of times a message can be received
        before giving up. Only enforced when ``dead_letter=False``.
      min_backoff: The minimum backoff in milliseconds. Defaults to 15 seconds.
      max_backoff: The maximum backoff in milliseconds. Defaults to 7 days.
      retry_when: An optional predicate ``(retries, exception) -> bool``
        that determines whether a task should be retried.
    """

    def __init__(
        self,
        *,
        max_retries: int = 20,
        min_backoff: int | None = None,
        max_backoff: int | None = None,
        retry_when=None,
    ):
        self.logger = get_logger(__name__, type(self))
        self.max_retries = max_retries
        self.min_backoff = min_backoff or DEFAULT_MIN_BACKOFF
        self.max_backoff = max_backoff or DEFAULT_MAX_BACKOFF
        self.retry_when = retry_when

    @property
    def actor_options(self):
        return {
            "max_retries",
            "min_backoff",
            "max_backoff",
            "retry_when",
            "throws",
            "on_retry_exhausted",
        }

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if exception is None:
            return

        actor = broker.get_actor(message.actor_name)

        throws = message.options.get("throws") or actor.options.get("throws")
        if throws and isinstance(exception, throws):
            self.logger.info("Aborting message %r.", message.message_id)
            message.fail()
            return

        receive_count = int(
            message._sqs_message.attributes.get("ApproximateReceiveCount", 1)
        )

        retry_when = actor.options.get("retry_when", self.retry_when)

        if broker.dead_letter:
            if retry_when is not None and not retry_when(receive_count - 1, exception):
                self.logger.warning(
                    "Retry predicate rejected message %r.", message.message_id
                )
                message._sqs_message.delete()
                message.fail()
                self._notify_retry_exhausted(broker, actor, message, receive_count - 1)
                return

            delay = self._compute_delay(actor, message, receive_count, exception)
            self.logger.info(
                "Retrying message %r (receive count: %d, visibility timeout: %ds).",
                message.message_id,
                receive_count,
                delay,
            )
            message._sqs_message.change_visibility(VisibilityTimeout=delay)
            message._sqs_nack_delete = False
            message.fail()
        else:
            max_retries = message.options.get(
                "max_retries",
                actor.options.get("max_retries", self.max_retries),
            )

            if retry_when is not None and not retry_when(receive_count - 1, exception):
                self.logger.warning(
                    "Retry predicate rejected message %r.", message.message_id
                )
                message.fail()
                self._notify_retry_exhausted(broker, actor, message, receive_count - 1)
                return

            if max_retries is not None and receive_count > max_retries:
                self.logger.warning(
                    "Retries exceeded for message %r (receive count: %d).",
                    message.message_id,
                    receive_count,
                )
                message.fail()
                self._notify_retry_exhausted(broker, actor, message, receive_count - 1)
                return

            delay = self._compute_delay(actor, message, receive_count, exception)
            self.logger.info(
                "Retrying message %r (receive count: %d, visibility timeout: %ds).",
                message.message_id,
                receive_count,
                delay,
            )
            message._sqs_message.change_visibility(VisibilityTimeout=delay)
            message._sqs_nack_delete = False
            message.fail()

    def after_skip_message(self, broker, message):
        message.fail()

    def _compute_delay(self, actor, message, receive_count, exception):
        if isinstance(exception, Retry) and exception.delay is not None:
            delay_ms = exception.delay
        else:
            min_backoff = message.options.get(
                "min_backoff",
                actor.options.get("min_backoff", self.min_backoff),
            )
            max_backoff = message.options.get(
                "max_backoff",
                actor.options.get("max_backoff", self.max_backoff),
            )
            max_backoff = min(max_backoff, DEFAULT_MAX_BACKOFF)
            _, delay_ms = compute_backoff(
                receive_count - 1, factor=min_backoff, max_backoff=max_backoff
            )

        delay_seconds = min(delay_ms // 1000, MAX_VISIBILITY_TIMEOUT_SECONDS)
        return max(delay_seconds, 0)

    def _notify_retry_exhausted(self, broker, actor, message, retries):
        target_actor_name = message.options.get(
            "on_retry_exhausted"
        ) or actor.options.get("on_retry_exhausted")
        if target_actor_name:
            max_retries = message.options.get(
                "max_retries",
                actor.options.get("max_retries", self.max_retries),
            )
            target_actor = broker.get_actor(target_actor_name)
            target_actor.send(
                message.asdict(),
                {
                    "retries": retries,
                    "traceback": traceback.format_exc(limit=30),
                    "max_retries": max_retries,
                },
            )
