# dramatiq_sqs

A [Dramatiq] broker that can be used with [Amazon SQS].

This backend has a number of limitations compared to the built-in
Redis and RMQ backends:

* the max amount of time messages can be delayed by is 15 minutes,
* messages can be at most 256KiB large and
* messages must be processed within 2 hours of being pulled, otherwise
  they will be redelivered.

The backend uses [boto3] under the hood.  For details on how
authorization works, check out its [docs].


## Installation

    pipenv install dramatiq_sqs


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


## License

dramatiq_sqs is licensed under Apache 2.0.  Please see
[LICENSE] for licensing details.


[Dramatiq]: https://dramatiq.io
[Amazon SQS]: https://aws.amazon.com/sqs/
[boto3]: https://boto3.readthedocs.io/en/latest/
[docs]: https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration
[LICENSE]: https://github.com/Bogdanp/dramatiq_sqs/blob/master/LICENSE
[ElasticMQ]: https://github.com/adamw/elasticmq
