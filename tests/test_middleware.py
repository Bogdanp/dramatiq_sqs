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
