import importlib.metadata

from .broker import SQSBroker
from .middleware import SQSRetries

__version__ = importlib.metadata.version("dramatiq_sqs")

__all__ = [
    "SQSBroker",
    "SQSRetries",
    "__version__",
]
