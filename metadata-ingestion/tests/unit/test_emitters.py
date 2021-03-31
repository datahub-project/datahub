import inspect

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter


def test_emitters_not_abstract():
    assert not inspect.isabstract(DatahubRestEmitter)
    assert not inspect.isabstract(DatahubKafkaEmitter)
