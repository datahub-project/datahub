from datahub.ingestion.source.kinesis.kinesis import KinesisRegionKey


class TestKinesisRegionKey:
    def test_platform_instance_affects_guid(self):
        """Two regional containers with the same region and env but different
        platform_instance produce different GUIDs. This is the load-bearing
        property: it's what lets the connector emit collision-free Region
        containers for the same region across multiple AWS accounts."""
        a = KinesisRegionKey(region="us-east-1", platform="kinesis", env="PROD")
        b = KinesisRegionKey(
            region="us-east-1", platform="kinesis", env="PROD", instance="prod-acct"
        )
        assert a.guid() != b.guid()

    def test_different_regions_produce_different_guids(self):
        """Two containers in the same account+env but different AWS regions
        must produce distinct URNs — otherwise multi-region ingestion would
        collide on a single container."""
        east = KinesisRegionKey(
            region="us-east-1", platform="kinesis", env="PROD", instance="acct-1"
        )
        west = KinesisRegionKey(
            region="us-west-2", platform="kinesis", env="PROD", instance="acct-1"
        )
        assert east.guid() != west.guid()
