from dramatiq_sqs import utils


def test_batched_splits_iterators_into_batches():
    # Given that I have a range from 0 to 12
    xs = range(13)

    # When I pass that range to batched with n of 2
    batches = utils.batched(xs, 2)

    # Then I should get back these batches
    assert list(batches) == [(0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12,)]
