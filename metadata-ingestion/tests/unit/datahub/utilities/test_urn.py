from datahub.utilities.urns.urn import guess_platform_name


def test_guess_platform_name():
    assert guess_platform_name("urn:li:corpuser:jdoe") is None
    assert (
        guess_platform_name(
            "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"
        )
        == "kafka"
    )
    assert guess_platform_name("urn:li:dataFlow:(airflow,dag_abc,PROD)") == "airflow"
    assert (
        guess_platform_name(
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_123)"
        )
        == "airflow"
    )
    assert guess_platform_name("urn:li:chart:(looker,baz1)") == "looker"
    assert guess_platform_name("urn:li:dashboard:(looker,baz)") == "looker"
    assert (
        guess_platform_name(
            "urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)"
        )
        == "science"
    )
