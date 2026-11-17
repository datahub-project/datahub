import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.file import read_metadata_file
from datahub.metadata.schema_classes import (
    BytesTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = pytest.mark.integration_batch_4


def test_mongodb_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mongodb"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mongo"
    ) as docker_services:
        wait_for_port(docker_services, "testmongodb", 27017)
        # Compose file exposes the mongo port ephemerally, so a leaked container
        # from a prior run can never hold onto the port a fresh run needs.
        mongo_port = docker_services.port_for("testmongodb", 27017)

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "mongodb-test",
                "source": {
                    "type": "mongodb",
                    "config": {
                        "connect_uri": f"mongodb://localhost:{mongo_port}",
                        "username": "mongoadmin",
                        "password": "examplepass",
                        "maxDocumentSize": 25000,
                        "platform_instance": "instance",
                        "schemaSamplingSize": None,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mongodb_mces.json",
                    },
                },
            }
        )
        assert isinstance(pipeline.config.source.config, dict)
        connection_config = {
            key: pipeline.config.source.config[key]
            for key in ("connect_uri", "username", "password")
        }
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "mongodb_mces.json",
            golden_path=test_resources_dir / "mongodb_mces_golden.json",
        )

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "mongodb-test-small-schema-size",
                "source": {
                    "type": "mongodb",
                    "config": {
                        **connection_config,
                        "maxSchemaSize": 10,
                        "platform_instance": "instance",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mongodb_mces_small_schema_size.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "mongodb_mces_small_schema_size.json",
            golden_path=test_resources_dir
            / "mongodb_mces_small_schema_size_golden.json",
        )

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "mongodb-test-no-random-sampling",
                "source": {
                    "type": "mongodb",
                    "config": {
                        **connection_config,
                        "useRandomSampling": False,
                        "platform_instance": "instance",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mongodb_mces_no_random_sampling.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "mongodb_mces_no_random_sampling.json",
            golden_path=test_resources_dir
            / "mongodb_mces_no_random_sampling_golden.json",
        )

        # Keep the default Binary coverage above and exercise UUID decoding through
        # the source's MongoClient options against the same subtype-4 seed value.
        standard_uuid_output = tmp_path / "mongodb_mces_standard_uuid.json"
        pipeline = Pipeline.create(
            {
                "run_id": "mongodb-test-standard-uuid",
                "source": {
                    "type": "mongodb",
                    "config": {
                        **connection_config,
                        "platform_instance": "instance",
                        "collection_pattern": {
                            "allow": [r"^mngdb\.nativeTypesCollection$"],
                        },
                        "schemaSamplingSize": None,
                        "options": {"uuidRepresentation": "standard"},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(standard_uuid_output)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        schemas = [
            event.aspect
            for event in read_metadata_file(standard_uuid_output)
            if isinstance(event, MetadataChangeProposalWrapper)
            and event.entityUrn
            == "urn:li:dataset:(urn:li:dataPlatform:mongodb,instance.mngdb.nativeTypesCollection,PROD)"
            and isinstance(event.aspect, SchemaMetadataClass)
        ]
        assert len(schemas) == 1
        fields = {field.fieldPath: field for field in schemas[0].fields}
        assert fields["uuidField"].nativeDataType == "uuid"
        assert isinstance(fields["uuidField"].type.type, StringTypeClass)
        assert fields["binaryData"].nativeDataType == "binary"
        assert isinstance(fields["binaryData"].type.type, BytesTypeClass)
