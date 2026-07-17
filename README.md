# dramatiq_sqs

> [!WARNING]  
> The project is under active development. There **will** be breaking changes before v1.0.0. Please make sure to pin the version when using this for anything important.

A [Dramatiq] broker that can be used with [Amazon SQS].

This backend has a number of limitations compared to the built-in
Redis and RMQ backends:

* the max amount of time messages can be delayed by is 15 minutes,
* messages can be at most 1MiB large by default (configurable via `max_message_size`) and
* messages must be processed within 12 hours of being pulled,
otherwise they will be redelivered.

The backend uses [boto3] under the hood.  For details on how
authorization works, check out its [docs].


## Installation

    pip install dramatiq_sqs


## Usage

``` python
import dramatiq

from dramatiq.middleware import AgeLimit, TimeLimit, Callbacks, Pipelines, Prometheus, Retries
from dramatiq_sqs import SQSBroker

broker = SQSBroker(
    namespace="dramatiq_sqs_tests",
    middleware=[
        Prometheus(),
        AgeLimit(),
        TimeLimit(),
        Callbacks(),
        Pipelines(),
        Retries(min_backoff=1000, max_backoff=900000, max_retries=96),
    ],
)
dramatiq.set_broker(broker)
```


## Usage with [ElasticMQ]

``` python
broker = SQSBroker(
    # ...
    endpoint_url="http://127.0.0.1:9324",
)
```

## Heartbeat (visibility timeout extension)

The heartbeat is **off by default**. When enabled, the consumer periodically
extends the SQS visibility timeout of in-flight messages via
`ChangeMessageVisibility`, which protects against two issues:

1. A long-running task whose runtime exceeds the queue's `VisibilityTimeout`
   would otherwise be redelivered to another worker while still being
   processed (duplicate execution).
2. With `prefetch > 1`, prefetched messages sit in the consumer's local buffer.
   Without heartbeats they can expire while waiting for a worker to pick them
   up, even if the queue's `VisibilityTimeout` looks generous compared to
   per-task processing time.

Because messages are tracked from the moment they are received (not from when
processing starts), both cases are covered. If a worker dies, heartbeats stop
and SQS makes the message visible again within `heartbeat_extension` seconds,
so you can set a short visibility window and still avoid mid-flight redelivery.

Enable it by setting `heartbeat_interval`:

``` python
broker = SQSBroker(
    # ...
    heartbeat_interval=30,        # extend visibility every 30s (None disables)
    heartbeat_extension=120,      # each beat pushes the deadline 120s forward
    heartbeat_max_extensions=60,  # runaway cap: drop after this many beats
)
```

`heartbeat_extension` must be greater than `heartbeat_interval` so a message
can't expire between beats. After `heartbeat_max_extensions` beats a message is
dropped from tracking and left to SQS to redeliver, so a stuck worker can't pin
it forever.

Beats run on one background thread per consumer. Because of the GIL, a worker
running CPU-bound work that never releases it can delay beats, so keep a wide
margin between `heartbeat_interval` and `heartbeat_extension` (the defaults
leave 90s of slack).

## Example IAM Policy

Here are the IAM permissions needed by Dramatiq:

``` json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sqs:CreateQueue",
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:DeleteMessageBatch",
                "sqs:SendMessage",
                "sqs:SendMessageBatch",
                "sqs:ChangeMessageVisibility"
            ],
            "Resource": ["*"]
        }
    ]
}
```

`sqs:ChangeMessageVisibility` covers both the batch requeue (used on worker
shutdown) and the heartbeat.

## License

`dramatiq_sqs` is licensed under Apache 2.0.


[Dramatiq]: https://dramatiq.io
[Amazon SQS]: https://aws.amazon.com/sqs/
[boto3]: https://boto3.readthedocs.io/en/latest/
[docs]: https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration
[LICENSE]: https://github.com/Bogdanp/dramatiq_sqs/blob/master/LICENSE
[ElasticMQ]: https://github.com/adamw/elasticmq
