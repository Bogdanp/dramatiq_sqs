import importlib.metadata

from .broker import SQSBroker
from .exceptions import MessageDelayTooLong, MessageTooLarge

__version__ = importlib.metadata.version("dramatiq_sqs")

__all__ = [
    "MessageDelayTooLong",
    "MessageTooLarge",
    "SQSBroker",
    "__version__",
]
