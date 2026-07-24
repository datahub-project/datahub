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

    def get_latest_version(self, subject):
        return _SchemaVersion(subject, version=3, schema_id=42)

    def get_version(self, subject, version):
        return _SchemaVersion(subject, version=version, schema_id=7)


class _Schema:
    def __init__(self, schema_str, schema_type="AVRO"):
        self.schema_str = schema_str
        self.schema_type = schema_type


class _SchemaVersion:
    def __init__(self, subject, version, schema_id):
        self.version = version
        self.schema_id = schema_id
        self.schema = _Schema(f'{{"type": "record", "name": "{subject}"}}')


class _Group:
    def __init__(self, group_id):
        self.group_id = group_id


class _GroupsListing:
    def __init__(self, groups):
        self.valid = groups


class _GroupsFuture:
    def __init__(self, groups):
        self._listing = _GroupsListing(groups)

    def result(self, timeout):
        return self._listing


class _Admin:
    def list_consumer_groups(self, request_timeout):
        return _GroupsFuture([_Group("consumer-b"), _Group("consumer-a")])


def _probe(admin=None):
    return KafkaMetadataProbe(
        consumer=_Consumer(), admin=admin or _Admin(), registry=_Registry()
    )


def test_topics_hides_internal_and_reports_counts():
    with _probe() as p:
        topics = p.topics()
    assert [t["name"] for t in topics] == ["events"]  # __consumer_offsets hidden
    assert topics[0] == {"name": "events", "partitions": 3, "replication": 2}


def test_subjects():
    with _probe() as p:
        assert p.subjects() == ["events-key", "events-value"]


def test_consumer_groups_returns_sorted_ids():
    with _probe() as p:
        assert p.consumer_groups() == ["consumer-a", "consumer-b"]


def test_consumer_groups_respects_limit():
    with _probe() as p:
        assert p.consumer_groups(limit=1) == ["consumer-a"]


def test_schema_latest_version():
    with _probe() as p:
        result = p.schema("events-value")
    assert result == {
        "subject": "events-value",
        "version": 3,
        "id": 42,
        "schema_type": "AVRO",
        "schema_str": '{"type": "record", "name": "events-value"}',
    }


def test_schema_explicit_version():
    with _probe() as p:
        result = p.schema("events-value", version="2")
    assert result["version"] == 2
    assert result["id"] == 7


def test_topics_command_names_registered():
    from datahub.ingestion.agent.probe_methods import _iter_specs

    commands = [c for c, _ in _iter_specs(KafkaMetadataProbe)]
    for expected in ["consumer_groups", "schema", "subjects", "topic_config", "topics"]:
        assert expected in commands
