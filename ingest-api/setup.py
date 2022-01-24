from setuptools import setup, find_packages
import os
import sys
from typing import Dict, Set
test = {
    # We currently require both Avro libraries. The codegen uses avro-python3 (above)
    # schema parsers at runtime for generating and reading JSON into Python objects.
    # At the same time, we use Kafka's AvroSerializer, which internally relies on
    # fastavro for serialization. We do not use confluent_kafka[avro], since it
    # is incompatible with its own dep on avro-python3.
    "black",
    "pytest",
    "flake8",
    "mypy",
    "freezegun",
    "isort"
}

setup(name="ingest_api", 
        packages=find_packages(),
        setup_requires=[],
        extras_require={
                "test": list(test),                
        },
)