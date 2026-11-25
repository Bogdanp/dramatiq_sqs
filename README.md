# dramatiq_sqs

> [!WARNING]  
> The project is under active development. There **will** be breaking changes before v1.0.0. Please make sure to pin the version when using this for anything important.

A [Dramatiq] broker that can be used with [Amazon SQS].

The backend uses [boto3] under the hood.  For details on how
authorization works, check out its [docs].


## Installation

    pip install dramatiq_sqs


## Usage

```python
import dramatiq

from dramatiq.middleware import (
    AgeLimit,
    TimeLimit,
    Callbacks,
    Pipelines,
    Prometheus,
    Retries,
)
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

## Dead-letter queues
DLQs are enabled and used by default.
Note that SQS-native "dead-letter queues" aka redrive policies are _not_ used.
These are normal queues and the nacks are handled client-side by Dramatiq.

DLQs may be disabled globally, in this case failed messages will simply get deleted:
```py
broker = SQSBroker(dl_queues_enabled=False)
```

Keep in mind that while you may still set up redrive policies externally, some
mechanisms like `Retries` and graceful shutdowns will not work as advertised
anymore, as Dramatiq can't have any control over SQS `ReceiveCount`s.

## Automatic queue provisioning
By default, in line with the behaviour of the built-in brokers, `dramatiq-sqs`
will attempt to create all declared queues in SQS if they don't exist and set the
queue attributes and tags. This includes DLQs, if enabled. None of the attribute
defaults are decided by `dramatiq-sqs`, nor is setting them required for normal
operation, so this is a purely optional customization mechanism.

```py
broker = SQSBroker(
    queue_attributes={"MaximumMessageSize": str(1024 * 1024)},
    dl_queue_attributes={
        "MaximumMessageSize": str(1024 * 1024),
        "MessageRetentionPeriod": str(24 * 3600),
    },
    tags={"managed-by": "dramatiq"},
)
```
[Supported Amazon SQS attributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html)

If all this is undesired (e.g. if the queues are defined statically via IaC),
queue management may be disabled completely:
```py
broker = SQSBroker(manage_queues=False)
```

## SQS IAM permissions required

Below is an example of an identity-based IAM policy that allows queue usage as
well as auto-provisioning. 

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "UseSQSQueues",
            "Effect": "Allow",
            "Action": [
                "sqs:ChangeMessageVisibility",
                "sqs:ChangeMessageVisibilityBatch",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:PurgeQueue",
                "sqs:ReceiveMessage",
                "sqs:SendMessage",
                "sqs:SendMessageBatch",
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "ManageSQSQueues",
            "Effect": "Allow",
            "Action": [
                "sqs:CreateQueue",
                "sqs:SetQueueAttributes",
                "sqs:TagQueue",
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
