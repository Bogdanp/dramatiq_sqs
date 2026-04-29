# dramatiq_sqs

> [!WARNING]  
> The project is under active development. There **will** be breaking changes before v1.0.0. Please make sure to pin the version when using this for anything important.

A [Dramatiq] broker that can be used with [Amazon SQS].

This backend has a number of limitations compared to the built-in
Redis and RMQ backends:

* the max amount of time messages can be delayed by is 15 minutes,
* messages can be at most 1MiB large and
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

By default, the broker keeps in-flight messages alive by periodically
extending their SQS visibility timeout via `ChangeMessageVisibility`.
This protects against two issues:

1. A long-running task whose runtime exceeds the queue's
   `VisibilityTimeout` would otherwise be redelivered to another worker
   while still being processed (duplicate execution).
2. With `prefetch > 1`, prefetched messages sit in the consumer's local
   buffer. Without heartbeats, they can expire while waiting for a
   worker to pick them up, even if the queue's `VisibilityTimeout` looks
   generous compared to per-task processing time.

If a worker dies, heartbeats stop and SQS makes the message visible
again to other consumers within `heartbeat_extension` seconds — typically
much shorter than the static `VisibilityTimeout` you'd otherwise need
to set defensively.

Knobs (constructor kwargs on `SQSBroker`):

* `heartbeat_interval` — how often (in seconds) to extend visibility.
  Pass `None` to disable heartbeats entirely. Defaults to 30 seconds.
* `heartbeat_extension` — how far each heartbeat pushes the visibility
  deadline. Must be greater than `heartbeat_interval`. Defaults to 120.
* `heartbeat_max_extensions` — runaway cap. After this many beats per
  message, the message is dropped from heartbeat tracking and SQS
  redelivers it. Defaults to 60 (so by default, a message can be kept
  alive for up to ~30 minutes total).

To disable:

``` python
broker = SQSBroker(
    # ...
    heartbeat_interval=None,
)
```


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
                "sqs:ChangeMessageVisibility",
                "sqs:ChangeMessageVisibilityBatch"
            ],
            "Resource": ["*"]
        }
    ]
}
```

`sqs:ChangeMessageVisibility` is required by the heartbeat (omit it only
if you also pass `heartbeat_interval=None`). `sqs:ChangeMessageVisibilityBatch`
is used by `consumer.requeue()`.

## License

`dramatiq_sqs` is licensed under Apache 2.0.


[Dramatiq]: https://dramatiq.io
[Amazon SQS]: https://aws.amazon.com/sqs/
[boto3]: https://boto3.readthedocs.io/en/latest/
[docs]: https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration
[LICENSE]: https://github.com/Bogdanp/dramatiq_sqs/blob/master/LICENSE
[ElasticMQ]: https://github.com/adamw/elasticmq
