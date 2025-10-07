import json
import time
import uuid

import dramatiq
import pytest
from botocore.stub import Stubber

from dramatiq_sqs import SQSBroker
from dramatiq_sqs.broker import MAX_MESSAGE_RETENTION


def test_can_enqueue_and_process_messages(broker, worker, queue_name):
    # Given that I have an actor that stores incoming messages in a database
    db = []

    @dramatiq.actor(queue_name=queue_name)
    def do_work(x):
        db.append(x)

    # When I send that actor a message
    do_work.send(1)

    # And wait for it to be processed
    time.sleep(1)

    # Then the db should contain that message
    assert db == [1]


def test_can_enqueue_and_process_messages_close_to_size_limit(
    broker, worker, queue_name
):
    # Given that I have an actor that stores incoming messages in a database
    db = []

    @dramatiq.actor(queue_name=queue_name)
    def do_work(x):
        db.append(x)

    # When I send that actor a message that is close to the size limit after base64 encoding
    # 256KiB is the limit. A 190KiB string becomes ~253KiB after base64 encoding.
    large_string = "a" * 190 * 1024
    do_work.send(large_string)

    # And wait for it to be processed
    time.sleep(2)

    # Then the db should contain that message
    assert db == [large_string]


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

    # And poll the database for a result each second
    for _ in range(60):
        if db:
            break

        time.sleep(1)

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
    # Then I should get back a ValueError
    with pytest.raises(ValueError):
        do_work.send_with_options(delay=3600000)


def test_cant_enqueue_messages_that_are_too_large(broker, queue_name):
    # Given that I have an actor
    @dramatiq.actor(queue_name=queue_name)
    def do_work(s):
        pass

    # When I attempt to send that actor a message that's too large after base64 encoding
    # Then a RuntimeError should be raised
    with pytest.raises(RuntimeError):
        do_work.send("a" * 768 * 1024)


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


def test_creates_dead_letter_queue():
    # Given that I have an SQS broker with dead letters turned on
    broker = SQSBroker(
        namespace="dramatiq_sqs_tests",
        dead_letter=True,
        max_receives=20,
    )

    # And I've stubbed out all the relevant API calls
    stubber = Stubber(broker.sqs.meta.client)
    queue_name = "dramatiq_sqs_tests_test"
    dlq_name = f"{queue_name}_dlq"
    queue_url = f"http://queue/{queue_name}"
    dlq_url = f"http://queue/{dlq_name}"
    dlq_arn = f"arn:aws:sqs:us-east-1:000:{dlq_name}"

    stubber.add_client_error(
        "get_queue_url", "QueueDoesNotExist", expected_params={"QueueName": queue_name}
    )
    stubber.add_response("create_queue", {"QueueUrl": queue_url})
    stubber.add_response(
        "get_queue_attributes",
        {"Attributes": {}},
        {"QueueUrl": queue_url, "AttributeNames": ["All"]},
    )
    stubber.add_client_error(
        "get_queue_url", "QueueDoesNotExist", expected_params={"QueueName": dlq_name}
    )
    stubber.add_response("create_queue", {"QueueUrl": dlq_url})
    stubber.add_response(
        "get_queue_attributes",
        {"Attributes": {"QueueArn": dlq_arn}},
        {"QueueUrl": dlq_url, "AttributeNames": ["All"]},
    )

    stubber.add_response(
        "set_queue_attributes",
        {},
        {
            "QueueUrl": queue_url,
            "Attributes": {
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "20"}
                )
            },
        },
    )

    # When I create a queue
    # Then a dead-letter queue should be created
    # And a redrive policy matching the queue and max receives should be added
    with stubber:
        broker.declare_queue("test")
        stubber.assert_no_pending_responses()


def test_tags_queues_on_create():
    # Given that I have an SQS broker with tags
    broker = SQSBroker(
        namespace="dramatiq_sqs_tests", tags={"key1": "value1", "key2": "value2"}
    )

    # And I've stubbed out all the relevant API calls
    stubber = Stubber(broker.sqs.meta.client)
    queue_name = "dramatiq_sqs_tests_test"
    queue_url = f"http://queue/{queue_name}"

    stubber.add_client_error(
        "get_queue_url", "QueueDoesNotExist", expected_params={"QueueName": queue_name}
    )
    stubber.add_response("create_queue", {"QueueUrl": queue_url})
    stubber.add_response(
        "get_queue_attributes",
        {"Attributes": {}},
        {"QueueUrl": queue_url, "AttributeNames": ["All"]},
    )
    stubber.add_response(
        "tag_queue",
        {},
        {"QueueUrl": queue_url, "Tags": {"key1": "value1", "key2": "value2"}},
    )

    # When I create a queue
    # Then the queue should have the specified tags
    with stubber:
        broker.declare_queue("test")
        stubber.assert_no_pending_responses()


# --- FIFO Queue Tests ---


def test_fifo_queue_is_created_correctly():
    broker = SQSBroker(namespace="dramatiq_sqs_tests")
    stubber = Stubber(broker.sqs.meta.client)
    queue_name = "test.fifo"
    prefixed_name = f"dramatiq_sqs_tests_{queue_name}"
    queue_url = f"http://queue/{prefixed_name}"

    stubber.add_client_error(
        "get_queue_url",
        "QueueDoesNotExist",
        expected_params={"QueueName": prefixed_name},
    )
    stubber.add_response(
        "create_queue",
        {"QueueUrl": queue_url},
        {
            "QueueName": prefixed_name,
            "Attributes": {
                "MessageRetentionPeriod": str(MAX_MESSAGE_RETENTION),
                "FifoQueue": "true",
            },
        },
    )
    stubber.add_response(
        "get_queue_attributes",
        {"Attributes": {"FifoQueue": "true"}},
        {"QueueUrl": queue_url, "AttributeNames": ["All"]},
    )

    with stubber:
        broker.declare_queue(queue_name)
        stubber.assert_no_pending_responses()


def test_fifo_queue_with_dead_letter_is_created_correctly():
    broker = SQSBroker(namespace="dramatiq_sqs_tests", dead_letter=True, max_receives=5)
    stubber = Stubber(broker.sqs.meta.client)
    queue_name = "test.fifo"
    prefixed_name = f"dramatiq_sqs_tests_{queue_name}"
    prefixed_base_name = "dramatiq_sqs_tests_test"
    dlq_name = f"{prefixed_base_name}_dlq.fifo"
    queue_url = f"http://queue/{prefixed_name}"
    dlq_url = f"http://queue/{dlq_name}"
    dlq_arn = f"arn:aws:sqs:us-east-1:000:{dlq_name}"

    stubber.add_client_error(
        "get_queue_url",
        "QueueDoesNotExist",
        expected_params={"QueueName": prefixed_name},
    )
    stubber.add_response("create_queue", {"QueueUrl": queue_url})
    stubber.add_response(
        "get_queue_attributes",
        {"Attributes": {"FifoQueue": "true"}},
        {"QueueUrl": queue_url, "AttributeNames": ["All"]},
    )
    stubber.add_client_error(
        "get_queue_url", "QueueDoesNotExist", expected_params={"QueueName": dlq_name}
    )
    stubber.add_response("create_queue", {"QueueUrl": dlq_url})
    stubber.add_response(
        "get_queue_attributes",
        {"Attributes": {"QueueArn": dlq_arn}},
        {"QueueUrl": dlq_url, "AttributeNames": ["All"]},
    )
    redrive_policy = json.dumps(
        {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"}
    )
    stubber.add_response(
        "set_queue_attributes",
        {},
        {"QueueUrl": queue_url, "Attributes": {"RedrivePolicy": redrive_policy}},
    )

    with stubber:
        broker.declare_queue(queue_name)
        stubber.assert_no_pending_responses()


def test_enqueue_to_fifo_queue_requires_message_group_id(broker, fifo_queue_name):
    @dramatiq.actor(queue_name=fifo_queue_name)
    def do_work():
        pass

    with pytest.raises(ValueError, match="must have a 'message_group_id'"):
        do_work.send()


def test_enqueue_to_fifo_queue_with_delay_raises_error(broker, fifo_queue_name):
    @dramatiq.actor(queue_name=fifo_queue_name)
    def do_work():
        pass

    with pytest.raises(ValueError, match="FIFO queues do not support message delays"):
        do_work.send_with_options(delay=1000, options={"message_group_id": "1"})


def test_fifo_messages_are_processed_in_order(broker, worker, fifo_queue_name):
    processed_messages = []

    @dramatiq.actor(queue_name=fifo_queue_name, max_retries=0)
    def process_ordered_message(x):
        processed_messages.append(x)
        time.sleep(0.1)

    group_id = f"group-{uuid.uuid4()}"
    for i in range(5):
        process_ordered_message.send_with_options(
            args=(i,), options={"message_group_id": group_id}
        )

    # Wait for messages to be processed. SQS can have some latency.
    time.sleep(5)

    assert processed_messages == [0, 1, 2, 3, 4]


def test_fifo_message_deduplication(broker, worker, fifo_queue_name):
    processed_messages = []

    @dramatiq.actor(queue_name=fifo_queue_name, max_retries=0)
    def process_dedup_message(x):
        processed_messages.append(x)

    group_id = f"group-{uuid.uuid4()}"
    dedup_id = f"dedup-id-{uuid.uuid4()}"
    process_dedup_message.send_with_options(
        args=(1,),
        options={"message_group_id": group_id, "message_deduplication_id": dedup_id},
    )
    process_dedup_message.send_with_options(
        args=(1,),
        options={"message_group_id": group_id, "message_deduplication_id": dedup_id},
    )

    time.sleep(3)

    assert len(processed_messages) == 1
    assert processed_messages == [1]
