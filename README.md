# dramatiq_sqs

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


## Dead-Letter Queues and Retries

Dramatiq's built-in `Retries` middleware re-enqueues a **new message** on
each retry and deletes the original.  This means SQS's
`ApproximateReceiveCount` never increments, so redrive policies never
trigger and messages never reach a dead-letter queue.

If you need DLQ support, use the `SQSRetries` middleware instead of the
core `Retries` middleware.  `SQSRetries` keeps the **same SQS message**
and uses visibility timeouts for backoff, allowing
`ApproximateReceiveCount` to increment naturally and SQS redrive
policies to work as expected.

``` python
import dramatiq

from dramatiq.middleware import AgeLimit, TimeLimit, Callbacks, Pipelines
from dramatiq_sqs import SQSBroker, SQSRetries

broker = SQSBroker(
    namespace="dramatiq_sqs_tests",
    middleware=[
        AgeLimit(),
        TimeLimit(),
        Callbacks(),
        Pipelines(),
        SQSRetries(min_backoff=1000, max_backoff=900000),
    ],
    dead_letter=True,
    max_receives=5,
)
dramatiq.set_broker(broker)
```

When `dead_letter=True`, the retry limit is controlled entirely by SQS's
`maxReceiveCount` (set via `max_receives`).  The middleware does not
enforce its own `max_retries`. SQS's `maxReceiveCount` is the single
source of truth for retry limits.

When `dead_letter=False`, the middleware uses `ApproximateReceiveCount`
to enforce `max_retries` and deletes the message once retries are
exhausted.

### on_retry_exhausted

The `on_retry_exhausted` callback only fires when `dead_letter=False`. When
`dead_letter=True`, SQS moves exhausted messages to the DLQ via the redrive
policy and dramatiq is not involved, so the callback is never triggered.

### Key differences from the core `Retries` middleware

* Retried messages keep the same SQS message ID (not re-enqueued)
* Backoff is implemented via visibility timeout (max 12 hours) instead of
  message delay (max 15 minutes)
* Message body is unchanged across retries (no traceback/retry metadata
  appended)
* `ApproximateReceiveCount` is used for retry tracking instead of
  `message.options["retries"]`

The IAM policy below includes `ChangeMessageVisibility` which is required
by `SQSRetries`.


## Usage with [ElasticMQ]

``` python
broker = SQSBroker(
    # ...
    endpoint_url="http://127.0.0.1:9324",
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
                "sqs:ChangeMessageVisibility"
            ],
            "Resource": ["*"]
        }
    ]
}
```

## License

`dramatiq_sqs` is licensed under Apache 2.0.


[Dramatiq]: https://dramatiq.io
[Amazon SQS]: https://aws.amazon.com/sqs/
[boto3]: https://boto3.readthedocs.io/en/latest/
[docs]: https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration
[LICENSE]: https://github.com/Bogdanp/dramatiq_sqs/blob/master/LICENSE
[ElasticMQ]: https://github.com/adamw/elasticmq
