from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import dramatiq

_ProxyT = TypeVar("_ProxyT", bound=dramatiq.MessageProxy)

if TYPE_CHECKING:

    class Consumer(dramatiq.Consumer, Generic[_ProxyT]):
        def ack(self, message: _ProxyT) -> None: ...  # ty: ignore[incompatible-method-override]
        def nack(self, message: _ProxyT) -> None: ...  # ty: ignore[incompatible-method-override]
        def requeue(self, messages: Iterable[_ProxyT]) -> None: ...  # ty: ignore[incompatible-method-override]

    class Broker(dramatiq.Broker):
        def consume(
            self, queue_name: str, prefetch: int = 1, timeout: int = 30000
        ) -> Consumer[Any]: ...
else:

    class Consumer(dramatiq.Consumer):
        def __class_getitem__(cls, item: str) -> None:
            return cls

    Broker = dramatiq.Broker
