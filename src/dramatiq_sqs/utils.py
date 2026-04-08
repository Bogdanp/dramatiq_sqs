import itertools
import sys
from collections.abc import Iterable
from typing import TypeVar

if sys.version_info >= (3, 12):
    batched = itertools.batched
else:
    _T = TypeVar("_T")

    def batched(iterable: Iterable[_T], n: int) -> Iterable[tuple[_T]]:
        iterator = iter(iterable)

        while batch := tuple(itertools.islice(iterator, n)):
            yield batch
