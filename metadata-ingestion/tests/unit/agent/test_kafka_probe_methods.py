from datahub.ingestion.source.kafka.kafka_probe import KafkaMetadataProbe


class _Part:
    def __init__(self, replicas):
        self.replicas = replicas


class _TopicMeta:
    def __init__(self, nparts, nrepl):
        self.partitions = {i: _Part([0] * nrepl) for i in range(nparts)}


class _ClusterMeta:
    def __init__(self):
        self.topics = {
            "events": _TopicMeta(3, 2),
            "__consumer_offsets": _TopicMeta(50, 1),
        }


class _Consumer:
    def list_topics(self, timeout):
        return _ClusterMeta()

    def close(self):
        pass


class _Registry:
    def get_subjects(self):
        return ["events-value", "events-key"]


def _probe():
    return KafkaMetadataProbe(consumer=_Consumer(), admin=None, registry=_Registry())


def test_topics_hides_internal_and_reports_counts():
    with _probe() as p:
        topics = p.topics()
    assert [t["name"] for t in topics] == ["events"]  # __consumer_offsets hidden
    assert topics[0] == {"name": "events", "partitions": 3, "replication": 2}


def test_subjects():
    with _probe() as p:
        assert p.subjects() == ["events-key", "events-value"]


def test_topics_command_names_registered():
    from datahub.ingestion.agent.probe_methods import _iter_specs

    commands = [c for c, _ in _iter_specs(KafkaMetadataProbe)]
    for expected in ["consumer_groups", "schema", "subjects", "topic_config", "topics"]:
        assert expected in commands
