import pytest

from dramatiq_sqs.dto import QueueName
from dramatiq_sqs.utils import build_queue_name, chunk


def test_chunk_can_split_iterators_into_chunks():
    # Given that I have a range from 0 to 12
    xs = range(13)

    # When I pass that range to chunk with a chunksize of 2
    chunks = chunk(xs, chunksize=2)

    # Then I should get back these chunks
    assert list(chunks) == [
        [0, 1],
        [2, 3],
        [4, 5],
        [6, 7],
        [8, 9],
        [10, 11],
        [12],
    ]


@pytest.mark.parametrize(
    "namespace,queue_name,expected",
    [
        ("namespace", "queue", QueueName(default="namespace_queue", dlq="namespace_queue_dlq")),
        ("", "queue", QueueName(default="queue", dlq="queue_dlq")),
        (None, "queue", QueueName(default="queue", dlq="queue_dlq")),
    ],
)
def test_should_return_a_queue_name(namespace, queue_name, expected):
    assert build_queue_name(queue_name=queue_name, namespace=namespace) == expected
