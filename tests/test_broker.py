import json
import time

import dramatiq
import pytest
from botocore.stub import Stubber

from dramatiq_sqs import SQSBroker


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

    # When I attempt to send that actor a message that's too large
    # Then a RuntimeError should be raised
    with pytest.raises(RuntimeError):
        do_work.send("a" * 512 * 1024)


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
    stubber.add_response("create_queue", {"QueueUrl": ""})
    stubber.add_response("create_queue", {"QueueUrl": ""})
    stubber.add_response("get_queue_attributes", {"Attributes": {"QueueArn": "dlq"}})
    stubber.add_response("set_queue_attributes", {}, {
        "QueueUrl": "",
        "Attributes": {
            "RedrivePolicy": json.dumps({
                "deadLetterTargetArn": "dlq",
                "maxReceiveCount": "20"
            })
        }
    })

    # When I create a queue
    # Then a dead-letter queue should be created
    # And a redrive policy matching the queue and max receives should be added
    with stubber:
        broker.declare_queue("test")
        stubber.assert_no_pending_responses()


def test_tags_queues_on_create():
    # Given that I have an SQS broker with tags
    broker = SQSBroker(
        namespace="dramatiq_sqs_tests",
        tags={"key1": "value1", "key2": "value2"}
    )

    # And I've stubbed out all the relevant API calls
    stubber = Stubber(broker.sqs.meta.client)
    stubber.add_response("create_queue", {"QueueUrl": ""})
    stubber.add_response("tag_queue", {}, {
        "QueueUrl": "",
        "Tags": {
            "key1": "value1",
            "key2": "value2"
        }
    })

    # When I create a queue
    # Then the queue should have the specified tags
    with stubber:
        broker.declare_queue("test")
        stubber.assert_no_pending_responses()
