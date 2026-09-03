import sys
from collections.abc import Callable
from typing import Generic, TypeVar

import dramatiq

if sys.version_info >= (3, 11):
    from typing import NamedTuple
else:
    from typing_extensions import NamedTuple


_Q = TypeVar("_Q")


class QueueSet(NamedTuple, Generic[_Q]):
    name: str
    queue: _Q
    dl_queue: _Q | None


class QueueSetRegistry(dict[str, QueueSet[_Q]]):
    def __init__(
        self,
        queuesets: dict[str, QueueSet[_Q]] | None = None,
        factory: Callable[[str], QueueSet[_Q]] | None = None,
    ) -> None:
        super().__init__(queuesets or ())
        self._names = set(self.keys())
        self._factory = factory

    def declare_queueset(self, name: str) -> None:
        self._names.add(name)

    @property
    def declared_queuesets(self) -> set[str]:
        return self._names

    def __missing__(self, name: str) -> QueueSet[_Q]:
        if name in self._names and self._factory is not None:
            queueset = self[name] = self._factory(name)
            return queueset

        raise dramatiq.QueueNotFound(name)
