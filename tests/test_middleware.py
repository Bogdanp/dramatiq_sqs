import time
import uuid

import dramatiq


class TestNackBehavior:
    def test_nack_deletes_by_default(self, broker, queue_name):
        @dramatiq.actor(queue_name=queue_name)
        def do_work():
            pass

        do_work.send()

        consumer = broker.consume(queue_name)
        message = next(consumer)
        assert message is not None

        consumer.nack(message)

        # Message should be deleted - next consume should return nothing
        second = next(consumer)
        assert second is None

    def test_nack_does_not_delete_when_flag_is_false(self, broker, queue_name):
        @dramatiq.actor(queue_name=queue_name)
        def do_work():
            pass

        do_work.send()

        consumer = broker.consume(queue_name, timeout=1000)
        message = next(consumer)
        assert message is not None

        message._sqs_nack_delete = False
        # Set visibility to 0 so the message is immediately available again
        message._sqs_message.change_visibility(VisibilityTimeout=0)
        consumer.nack(message)

        # Message should still be in the queue
        second = next(consumer)
        assert second is not None
        assert second.message_id == message.message_id


class TestSQSRetriesWithDLQ:
    def test_retries_message_via_sqs_redelivery(self, sqs_retries_broker):
        queue_name = f"queue_{uuid.uuid4()}"
        attempts = []

        @dramatiq.actor(queue_name=queue_name)
        def failing_work():
            attempts.append(1)
            raise RuntimeError("fail")

        failing_work.send()

        worker = dramatiq.Worker(sqs_retries_broker)
        worker.start()

        try:
            time.sleep(15)
        finally:
            worker.stop()

        # Should have been retried multiple times (receive count incrementing)
        assert len(attempts) > 1

    def test_successful_message_is_acked(self, sqs_retries_broker):
        queue_name = f"queue_{uuid.uuid4()}"
        db = []

        @dramatiq.actor(queue_name=queue_name)
        def do_work(x):
            db.append(x)

        do_work.send(42)

        worker = dramatiq.Worker(sqs_retries_broker)
        worker.start()

        try:
            sqs_retries_broker.join(queue_name, timeout=30000)
        finally:
            worker.stop()

        assert db == [42]

    def test_throws_exception_is_not_retried(self, sqs_retries_broker):
        queue_name = f"queue_{uuid.uuid4()}"
        attempts = []

        @dramatiq.actor(queue_name=queue_name, throws=(ValueError,))
        def throwing_work():
            attempts.append(1)
            raise ValueError("expected")

        throwing_work.send()

        worker = dramatiq.Worker(sqs_retries_broker)
        worker.start()

        try:
            time.sleep(10)
        finally:
            worker.stop()

        # Should only have been attempted once (not retried)
        assert len(attempts) == 1

    def test_message_reaches_dlq(self, sqs_retries_broker):
        queue_name = f"queue_{uuid.uuid4()}"

        @dramatiq.actor(queue_name=queue_name)
        def always_fails():
            raise RuntimeError("permanent failure")

        always_fails.send()

        worker = dramatiq.Worker(sqs_retries_broker)
        worker.start()

        try:
            # Wait long enough for max_receives (3) to be exhausted
            time.sleep(20)
        finally:
            worker.stop()

        # Check the DLQ has the message
        namespace = sqs_retries_broker.namespace
        dlq_name = f"{namespace}_{queue_name}_dlq"
        dlq = sqs_retries_broker.sqs.get_queue_by_name(QueueName=dlq_name)
        dlq.load()
        dlq_count = int(dlq.attributes["ApproximateNumberOfMessages"])
        assert dlq_count >= 1


class TestSQSRetriesWithoutDLQ:
    def test_retries_then_deletes_on_exhaustion(self, sqs_retries_broker_no_dlq):
        queue_name = f"queue_{uuid.uuid4()}"
        attempts = []

        @dramatiq.actor(queue_name=queue_name, max_retries=2)
        def failing_work():
            attempts.append(1)
            raise RuntimeError("fail")

        failing_work.send()

        worker = dramatiq.Worker(sqs_retries_broker_no_dlq)
        worker.start()

        try:
            time.sleep(15)
        finally:
            worker.stop()

        # Should have been attempted at least twice
        assert len(attempts) >= 2

        # Queue should be empty (message deleted after retries exhausted)
        queue = sqs_retries_broker_no_dlq.queues[queue_name]
        queue.load()
        remaining = int(queue.attributes["ApproximateNumberOfMessages"])
        assert remaining == 0

    def test_approximate_receive_count_is_available(self, sqs_retries_broker_no_dlq):
        queue_name = f"queue_{uuid.uuid4()}"

        @dramatiq.actor(queue_name=queue_name)
        def do_work():
            pass

        do_work.send()

        consumer = sqs_retries_broker_no_dlq.consume(queue_name)
        message = next(consumer)
        assert message is not None

        receive_count = message._sqs_message.attributes.get("ApproximateReceiveCount")
        assert receive_count is not None
        assert int(receive_count) >= 1

        consumer.ack(message)
