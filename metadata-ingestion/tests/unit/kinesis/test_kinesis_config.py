from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig


class TestKinesisConfig:
    def test_default_config(self):
        # Default config requires nothing mandatory
        config = KinesisSourceConfig()
        assert config.connection is not None
        assert config.stream_pattern.allowed("test")
        assert config.region_name is None

    def test_region_override(self):
        config = KinesisSourceConfig(region_name="us-west-2")
        assert config.region_name == "us-west-2"

    def test_connection_override(self):
        config = KinesisSourceConfig(connection={"aws_region": "eu-central-1"})
        assert config.connection.aws_region == "eu-central-1"

    def test_pattern_filtering(self):
        config = KinesisSourceConfig(stream_pattern={"deny": ["^internal.*"]})
        assert config.stream_pattern.allowed("public_stream")
        assert not config.stream_pattern.allowed("internal_stream")
