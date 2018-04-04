import os

from setuptools import setup


def rel(*xs):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *xs)


with open(rel("dramatiq_sqs", "__init__.py"), "r") as f:
    version_marker = "__version__ = "
    for line in f:
        if line.startswith(version_marker):
            _, version = line.split(version_marker)
            version = version.strip().strip('"')
            break
    else:
        raise RuntimeError("Version marker not found.")

setup(
    name="dramatiq_sqs",
    version=version,
    description="An Amazon SQS broker for Dramatiq.",
    long_description="Visit https://github.com/Bogdanp/dramatiq_sqs for more information.",
    packages=["dramatiq_sqs"],
    include_package_data=True,
    install_requires=["boto3", "dramatiq"],
    python_requires=">=3.5",
)
