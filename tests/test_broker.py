import contextlib
import time
import uuid
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

import dramatiq
import pytest
from dramatiq.middleware import AgeLimit, Callbacks, Pipelines, Retries, TimeLimit

from dramatiq_sqs import SQSBroker
from dramatiq_sqs.exceptions import MessageDelayTooLong, MessageTooLarge

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import SQSServiceResource


def _make_broker(
    elasticmq_endpoint_url: str,
    namespace: str,
    tags: dict[str, str],
    **kwargs: Any,
) -> SQSBroker:
    """Build an ad-hoc broker for tests that need custom kwargs.

    Mirrors the conftest ``broker`` fixture but lets each test override knobs
    like ``visibility_timeout`` or the heartbeat settings.
    """
    return SQSBroker(
        namespace=namespace,
        middleware=[
            AgeLimit(),
            TimeLimit(),
            Callbacks(),
            Pipelines(),
            Retries(min_backoff=1000, max_backoff=900000, max_retries=96),
        ],
        tags=tags,
        region_name="eu-central-1",
        endpoint_url=elasticmq_endpoint_url,
        aws_access_key_id="000000000000",
        aws_secret_access_key="000000000000",
        **kwargs,
    )


@contextlib.contextmanager
def _broker_session(broker: SQSBroker) -> Iterator[SQSBroker]:
    """Set as the global broker, yield, then clean up the queues on exit."""
    dramatiq.set_broker(broker)
    try:
        yield broker
    finally:
        for queue in broker.queues.values():
            queue.delete()


def test_can_enqueue_and_process_messages(broker, worker, queue_name):
    # Given that I have an actor that stores incoming messages in a database
    db = []

    @dramatiq.actor(queue_name=queue_name)
    def do_work(x):
        db.append(x)

    # When I send that actor a message
    do_work.send(1)

    # And wait for it to be processed
    broker.join(queue_name)

    # Then the db should contain that message
    assert db == [1]


def test_failed_messages_are_deleted_from_queue(broker, worker, queue_name):
    @dramatiq.actor(queue_name=queue_name, max_retries=0)
    def do_work():
        raise RuntimeError()

    do_work.send()

    broker.join(queue_name)


@pytest.mark.parametrize("dead_letter", [True])
@pytest.mark.parametrize(("max_retries", "attempts"), [(0, 5), (3, 5)])
def test_failed_messages_are_sent_to_dlq(
    broker, worker, queue_name, max_retries, attempts
):
    @dramatiq.actor(queue_name=queue_name, max_retries=max_retries)
    def do_work():
        raise RuntimeError()

    for _ in range(attempts):
        do_work.send()

    broker.join(queue_name)

    dlq = broker.dead_letter_queues[queue_name]
    messages = dlq.receive_messages(MaxNumberOfMessages=10)
    assert len(messages) == attempts


def test_limits_prefetch_while_if_queue_is_full(broker, worker, queue_name):
    # Given that I have an actor that stores incoming messages in a database
    db = []

    # Set the worker prefetch limit to 1
    worker.queue_prefetch = 1

    # Add delay to actor logic to simulate processing time
    @dramatiq.actor(queue_name=queue_name)
    def do_work(x):
        db.append(x)
        time.sleep(10)

    # When I send that actor messages, it'll only prefetch and process a single message
    do_work.send(1)
    do_work.send(2)

    # Wait for message to be processed
    time.sleep(2)

    # Then the db should contain only that message, while it sleeps
    assert db == [1]


def test_can_enqueue_delayed_messages(broker, worker, queue_name):
    # Given that I have an actor that stores incoming messages in a database
    db = []

    @dramatiq.actor(queue_name=queue_name)
    def do_work(x):
        db.append(x)

    # When I send that actor a delayed message
    start_time = time.time()
    do_work.send_with_options(args=(1,), delay=5000)

    broker.join(queue_name)

    # Then the db should contain that message
    assert db == [1]

    # And an appropriate amount of time should have passed
    delta = time.time() - start_time
    assert delta >= 5


def test_cant_delay_messages_for_longer_than_15_seconds(broker, queue_name):
    # Given that I have an actor
    @dramatiq.actor(queue_name=queue_name)
    def do_work():
        pass

    # When I attempt to send that actor a message farther than 15 minutes into the future
    # Then I should get back a MessageDelayTooLong
    with pytest.raises(MessageDelayTooLong):
        do_work.send_with_options(delay=3600000)


def test_cant_enqueue_messages_that_are_too_large(broker, queue_name):
    # Given that I have an actor
    @dramatiq.actor(queue_name=queue_name)
    def do_work(s):
        pass

    # When I attempt to send that actor a message that's too large after base64 encoding
    # Then a MessageTooLarge should be raised
    with pytest.raises(MessageTooLarge):
        do_work.send("a" * 768 * 1024)


def test_max_message_size_is_configurable(broker, queue_name):
    # Given that I lower the broker's max message size
    broker.max_message_size = 1024

    @dramatiq.actor(queue_name=queue_name)
    def do_work(s):
        pass

    # When I attempt to send a message that exceeds the configured limit
    # Then a MessageTooLarge should be raised
    with pytest.raises(MessageTooLarge):
        do_work.send("a" * 2048)


def test_max_message_size_is_validated():
    # When I attempt to instantiate a broker with a non-positive max message size
    # Then a ValueError should be raised
    with pytest.raises(ValueError):
        SQSBroker(max_message_size=0)


def test_retention_period_is_validated():
    # When I attempt to instantiate a broker with an invalid retention period
    # Then a ValueError should be raised
    with pytest.raises(ValueError):
        SQSBroker(retention=30 * 86400)


def test_can_requeue_consumed_messages(broker, queue_name):
    # Given that I have an actor
    @dramatiq.actor(queue_name=queue_name)
    def do_work():
        pass

    # When I send that actor a message
    do_work.send()

    # And consume the message off the queue
    consumer = broker.consume(queue_name)
    first_message = next(consumer)

    # And requeue the message
    consumer.requeue([first_message])

    # Then I should be able to consume the message again immediately
    second_message = next(consumer)
    assert first_message == second_message


def test_consumer_backs_off_when_prefetch_limit_reached(broker, queue_name):
    # Given an actor and a consumer with prefetch=1
    @dramatiq.actor(queue_name=queue_name)
    def do_work():
        pass

    # When I send a message and consume it (without acking)
    do_work.send()
    consumer = broker.consume(queue_name, prefetch=1, timeout=1000)
    in_flight = next(consumer)
    assert in_flight is not None
    assert consumer.message_refc == 1

    # And then call __next__ repeatedly while the prefetch limit is reached.
    deadline = time.monotonic() + 1.0
    calls = 0
    while time.monotonic() < deadline:
        result = next(consumer)
        assert result is None
        calls += 1

    # Then the consumer must have backed off so the number of __next__
    # calls in 1 second stays small instead of spinning.
    assert calls < 50

    consumer.ack(in_flight)


def test_close_requeues_prefetched_messages(broker, queue_name):
    # Given that I have an actor and a handful of pending messages
    @dramatiq.actor(queue_name=queue_name)
    def do_work():
        pass

    for _ in range(3):
        do_work.send()

    # When I start consuming with a prefetch large enough to pull them all
    # into the internal buffer at once
    consumer = broker.consume(queue_name, prefetch=5)
    first = next(consumer)
    assert first is not None

    # Then there should be buffered messages that haven't been returned yet
    buffered_count = len(consumer.messages)
    assert buffered_count >= 1

    # When I close the consumer
    consumer.close()

    # Then the internal buffer is drained
    assert len(consumer.messages) == 0

    # And a fresh consumer can read the buffered messages immediately —
    # meaning they were requeued, not left invisible until VisibilityTimeout.
    new_consumer = broker.consume(queue_name, prefetch=5)
    for _ in range(buffered_count):
        assert next(new_consumer) is not None


@pytest.fixture(params=["consume", "enqueue"])
def ensure_queue_trigger(
    request: pytest.FixtureRequest, broker: SQSBroker
) -> Callable[[str], Any]:
    match request.param:
        case "consume":
            return lambda queue_name: broker.consume(queue_name)
        case "enqueue":
            return lambda queue_name: broker.enqueue(
                dramatiq.Message(queue_name, "test", (), {}, {})
            )

    assert False, "invalid fixture param"


@pytest.mark.parametrize(
    ("queue_name", "dead_letter", "sqs_queue_names"),
    [
        ("queue42", False, ["{namespace}_queue42"]),
        ("queue42", True, ["{namespace}_queue42", "{namespace}_queue42_dlq"]),
    ],
)
def test_declare_queue(
    queue_name: str,
    sqs_queue_names: list[str],
    broker: SQSBroker,
    namespace: str,
    tags: dict[str, str],
    sqs: "SQSServiceResource",
    ensure_queue_trigger: Callable[[str], Any],
) -> None:
    broker.declare_queue(queue_name)

    assert broker.get_declared_queues() == {queue_name}
    assert len(list(sqs.queues.all())) == 0

    ensure_queue_trigger(queue_name)

    for sqs_queue_name in sqs_queue_names:
        sqs_queue = sqs.get_queue_by_name(
            QueueName=sqs_queue_name.format(namespace=namespace)
        )

        assert sqs.meta.client.list_queue_tags(QueueUrl=sqs_queue.url)["Tags"] == tags


# --- Heartbeat ---


def test_heartbeat_extension_must_exceed_interval():
    # When extension <= interval, a message could expire between beats.
    with pytest.raises(ValueError, match="heartbeat_extension"):
        SQSBroker(heartbeat_interval=60, heartbeat_extension=60)
    with pytest.raises(ValueError, match="heartbeat_extension"):
        SQSBroker(heartbeat_interval=60, heartbeat_extension=30)


def test_heartbeat_disabled_by_default(broker, queue_name):
    # Given the default broker (no heartbeat configured)
    @dramatiq.actor(queue_name=queue_name)
    def do_work(x):
        pass

    do_work.send(1)

    consumer = broker.consume(queue_name)
    try:
        msg = next(consumer)
        assert msg is not None
        # No ticker thread and nothing tracked.
        assert consumer.heartbeat.enabled is False
        assert consumer.heartbeat._ticker is None  # type: ignore[attr-defined]
        assert consumer.heartbeat._tracked == {}  # type: ignore[attr-defined]
    finally:
        consumer.ack(msg)
        consumer.close()


def test_heartbeat_tracks_from_receive_until_ack(
    elasticmq_endpoint_url, namespace, tags
):
    # Given a broker with the heartbeat enabled
    with _broker_session(
        _make_broker(
            elasticmq_endpoint_url,
            namespace,
            tags,
            heartbeat_interval=1,
            heartbeat_extension=30,
        )
    ) as broker:
        queue_name = f"queue_{uuid.uuid4()}"

        @dramatiq.actor(queue_name=queue_name)
        def do_work(x):
            pass

        do_work.send(1)
        do_work.send(2)
        # Give SQS a moment so both messages are available in one receive.
        time.sleep(0.5)

        consumer = broker.consume(queue_name, prefetch=2)
        try:
            msg1 = next(consumer)
            msg2 = next(consumer)
            assert msg1 is not None
            assert msg2 is not None

            tracked = consumer.heartbeat._tracked  # type: ignore[attr-defined]

            # Both are tracked from the moment they're received.
            assert msg1.message_id in tracked
            assert msg2.message_id in tracked

            # Acking one untracks only that message.
            consumer.ack(msg1)
            assert msg1.message_id not in tracked
            assert msg2.message_id in tracked

            consumer.ack(msg2)
            assert msg2.message_id not in tracked
        finally:
            consumer.close()


def test_heartbeat_extends_visibility_for_long_running_task(
    elasticmq_endpoint_url, namespace, tags
):
    # Given a visibility window shorter than the task's runtime, but with the
    # heartbeat keeping the message in flight.
    with _broker_session(
        _make_broker(
            elasticmq_endpoint_url,
            namespace,
            tags,
            visibility_timeout=3,
            heartbeat_interval=1,
            heartbeat_extension=3,
        )
    ) as broker:
        queue_name = f"queue_{uuid.uuid4()}"

        db: list[int] = []

        @dramatiq.actor(queue_name=queue_name, max_retries=0)
        def slow_work(x: int) -> None:
            db.append(x)
            time.sleep(6)  # twice the visibility window

        worker = dramatiq.Worker(broker)
        worker.start()
        try:
            slow_work.send(1)
            broker.join(queue_name, timeout=15000)

            # Processed exactly once. The heartbeat kept it invisible for the
            # whole 6s sleep.
            assert db == [1]
        finally:
            worker.stop()


def test_without_heartbeat_long_running_task_is_redelivered(
    elasticmq_endpoint_url, namespace, tags
):
    # Control for the test above: with the heartbeat off and a short visibility
    # window, SQS redelivers the message mid-flight and it runs more than once.
    with _broker_session(
        _make_broker(
            elasticmq_endpoint_url,
            namespace,
            tags,
            visibility_timeout=3,
            heartbeat_interval=None,
        )
    ) as broker:
        queue_name = f"queue_{uuid.uuid4()}"

        db: list[int] = []

        @dramatiq.actor(queue_name=queue_name, max_retries=0)
        def slow_work(x: int) -> None:
            db.append(x)
            time.sleep(6)

        worker = dramatiq.Worker(broker)
        worker.start()
        try:
            slow_work.send(1)
            # Long enough for the original run plus at least one redelivery.
            time.sleep(12)
            assert len(db) >= 2
        finally:
            worker.stop()
