from dramatiq_sqs.broker import chunk


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
