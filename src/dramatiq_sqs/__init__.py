import importlib.metadata

from .broker import SQSBroker

__version__ = importlib.metadata.version("dramatiq_sqs")

__all__ = [
    "SQSBroker",
    "__version__",
]
