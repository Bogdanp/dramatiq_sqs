# dramatiq_sqs

> [!WARNING]  
> The project is under active development. There **will** be breaking changes before v1.0.0. Please make sure to pin the version when using this for anything important.

A [Dramatiq] broker that can be used with [Amazon SQS].

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
                "sqs:SendMessageBatch"
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
