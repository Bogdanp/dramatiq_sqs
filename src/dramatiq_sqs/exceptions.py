class MessageTooLarge(RuntimeError):
    """Raised when a message exceeds the SQS maximum message size."""


class MessageDelayTooLong(ValueError):
    """Raised when a message delay exceeds the SQS maximum delay."""
