from dataclasses import dataclass


@dataclass
class QueueName:
    default: str
    dlq: str
