import json
import pathlib
from typing import List

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.file import read_metadata_file
from datahub.metadata.schema_classes import (
    AuditStampClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    MetadataChangeProposalClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SystemMetadataClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.testing import mce_helpers, pytest_hooks
from datahub.testing.compare_metadata_json import (
    MetadataJson,
    assert_metadata_files_equal,
    diff_metadata_json,
)

basic_1 = json.loads(
    """[
    {
        "auditHeader": null,
        "proposedSnapshot": {
            "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
                "aspects": [
                    {
                        "com.linkedin.pegasus2avro.common.Ownership": {
                            "owners": [
                                {
                                    "owner": "urn:li:corpuser:jdoe",
                                    "type": "DATAOWNER",
                                    "source": null
                                }
                            ],
                            "lastModified": {
                                "time": 1581407189000,
                                "actor": "urn:li:corpuser:jdoe",
                                "impersonator": null
                            }
                        }
                    },
                    {
                        "com.linkedin.pegasus2avro.common.InstitutionalMemory": {
                            "elements": [
                                {
                                    "url": "https://www.linkedin.com",
                                    "description": "Sample doc",
                                    "createStamp": {
                                        "time": 1581407189000,
                                        "actor": "urn:li:corpuser:jdoe",
                                        "impersonator": null
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        },
        "proposedDelta": null
    }
]"""
)

# Timestamps changed from basic_1 but same otherwise.
basic_2 = json.loads(
    """[
    {
        "auditHeader": null,
        "proposedSnapshot": {
            "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
                "aspects": [
                    {
                        "com.linkedin.pegasus2avro.common.Ownership": {
                            "owners": [
                                {
                                    "owner": "urn:li:corpuser:jdoe",
                                    "type": "DATAOWNER",
                                    "source": null
                                }
                            ],
                            "lastModified": {
                                "time": 1581407199000,
                                "actor": "urn:li:corpuser:jdoe",
                                "impersonator": null
                            }
                        }
                    },
                    {
                        "com.linkedin.pegasus2avro.common.InstitutionalMemory": {
                            "elements": [
                                {
                                    "url": "https://www.linkedin.com",
                                    "description": "Sample doc",
                                    "createStamp": {
                                        "time": 1581407389000,
                                        "actor": "urn:li:corpuser:jdoe",
                                        "impersonator": null
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        },
        "proposedDelta": null
    }
]"""
)

# Dataset owner changed from basic_2.
basic_3 = json.loads(
    """[
    {
        "auditHeader": null,
        "proposedSnapshot": {
            "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
                "aspects": [
                    {
                        "com.linkedin.pegasus2avro.common.Ownership": {
                            "owners": [
                                {
                                    "owner": "urn:li:corpuser:datahub",
                                    "type": "DATAOWNER",
                                    "source": null
                                }
                            ],
                            "lastModified": {
                                "time": 1581407199000,
                                "actor": "urn:li:corpuser:jdoe",
                                "impersonator": null
                            }
                        }
                    },
                    {
                        "com.linkedin.pegasus2avro.common.InstitutionalMemory": {
                            "elements": [
                                {
                                    "url": "https://www.linkedin.com",
                                    "description": "Sample doc",
                                    "createStamp": {
                                        "time": 1581407389000,
                                        "actor": "urn:li:corpuser:jdoe",
                                        "impersonator": null
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        },
        "proposedDelta": null
    }
]"""
)


def test_basic_diff_same() -> None:
    assert not diff_metadata_json(basic_1, basic_2, mce_helpers.IGNORE_PATH_TIMESTAMPS)


def test_basic_diff_only_owner_change() -> None:
    with pytest.raises(AssertionError):
        assert not diff_metadata_json(
            basic_2, basic_3, mce_helpers.IGNORE_PATH_TIMESTAMPS
        )


def test_basic_diff_owner_change() -> None:
    with pytest.raises(AssertionError):
        assert not diff_metadata_json(
            basic_1, basic_3, mce_helpers.IGNORE_PATH_TIMESTAMPS
        )


def _write_json(path: pathlib.Path, metadata: MetadataJson) -> pathlib.Path:
    path.write_text(json.dumps(metadata))
    return path


def _basic_1_as_mcps() -> List[MetadataChangeProposalWrapper]:
    urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"
    stamp = AuditStampClass(time=1581407189000, actor="urn:li:corpuser:jdoe")
    return [
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:jdoe", type=OwnershipTypeClass.DATAOWNER
                    )
                ],
                lastModified=stamp,
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=InstitutionalMemoryClass(
                elements=[
                    InstitutionalMemoryMetadataClass(
                        url="https://www.linkedin.com",
                        description="Sample doc",
                        createStamp=stamp,
                    )
                ]
            ),
        ),
    ]


def test_mce_golden_matches_equivalent_mcp_output(tmp_path: pathlib.Path) -> None:
    golden = _write_json(tmp_path / "golden.json", basic_1)
    output = tmp_path / "output.json"
    write_metadata_file(output, _basic_1_as_mcps())
    assert_metadata_files_equal(output, golden, golden_mces_as_mcps=True)


def test_mce_golden_differs_from_mcp_output_by_default(
    tmp_path: pathlib.Path,
) -> None:
    golden = _write_json(tmp_path / "golden.json", basic_1)
    output = tmp_path / "output.json"
    write_metadata_file(output, _basic_1_as_mcps())
    with pytest.raises(pytest.fail.Exception):
        assert_metadata_files_equal(output, golden)


def test_mce_golden_fails_on_missing_aspect_in_mcp_output(
    tmp_path: pathlib.Path,
) -> None:
    golden = _write_json(tmp_path / "golden.json", basic_1)
    output = tmp_path / "output.json"
    write_metadata_file(output, _basic_1_as_mcps()[:1])
    with pytest.raises(pytest.fail.Exception, match="institutionalMemory"):
        assert_metadata_files_equal(output, golden, golden_mces_as_mcps=True)


def test_mce_left_in_output_fails(tmp_path: pathlib.Path) -> None:
    golden = _write_json(tmp_path / "golden.json", basic_1)
    output = _write_json(tmp_path / "output.json", basic_1)
    with pytest.raises(pytest.fail.Exception):
        assert_metadata_files_equal(output, golden, golden_mces_as_mcps=True)


def test_fail_on_output_mces_fails_even_when_golden_matches(
    tmp_path: pathlib.Path,
) -> None:
    golden = _write_json(tmp_path / "golden.json", basic_1)
    output = tmp_path / "output.json"
    write_metadata_file(output, read_metadata_file(golden))
    assert_metadata_files_equal(output, golden)
    with pytest.raises(pytest.fail.Exception, match="MCE"):
        assert_metadata_files_equal(output, golden, fail_on_output_mces=True)


def test_fail_on_output_mces_blocks_update_golden(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    golden = tmp_path / "golden.json"
    output = _write_json(tmp_path / "output.json", basic_1)
    monkeypatch.setattr(
        pytest_hooks,
        "_settings",
        pytest_hooks.GoldenFileSettings(update_golden=True, copy_output=False),
    )
    with pytest.raises(pytest.fail.Exception, match="MCE"):
        assert_metadata_files_equal(output, golden, fail_on_output_mces=True)
    assert not golden.exists()


def test_fail_on_output_mces_passes_mcp_only_output(tmp_path: pathlib.Path) -> None:
    golden = _write_json(tmp_path / "golden.json", basic_1)
    output = tmp_path / "output.json"
    write_metadata_file(output, _basic_1_as_mcps())
    assert_metadata_files_equal(
        output, golden, golden_mces_as_mcps=True, fail_on_output_mces=True
    )


def test_mce_golden_ignore_paths_v2_with_ordered_compare(
    tmp_path: pathlib.Path,
) -> None:
    urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"

    def description_patch(description: str) -> List[MetadataChangeProposalClass]:
        return list(DatasetPatchBuilder(urn).set_description(description).build())

    # Real goldens and outputs carry systemMetadata from different runs.
    golden_mce = {**basic_1[0], "systemMetadata": {"lastObserved": 1, "runId": "a"}}
    golden = tmp_path / "golden.json"
    golden.write_text(
        json.dumps([golden_mce, *[p.to_obj() for p in description_patch("old")]])
    )
    output_mcps = _basic_1_as_mcps()
    for mcp in output_mcps:
        mcp.systemMetadata = SystemMetadataClass(lastObserved=2, runId="b")
    output = tmp_path / "output.json"
    write_metadata_file(output, [*output_mcps, *description_patch("new")])
    assert_metadata_files_equal(
        output,
        golden,
        ignore_paths_v2=["/description"],
        ignore_order=False,
        golden_mces_as_mcps=True,
    )


def test_unparseable_mce_golden_fails_with_diff(tmp_path: pathlib.Path) -> None:
    broken = json.loads(json.dumps(basic_1))
    snapshot = broken[0]["proposedSnapshot"]
    aspects = next(iter(snapshot.values()))["aspects"]
    aspects.append({"com.linkedin.pegasus2avro.common.Status": {"removed": "nope"}})
    golden = _write_json(tmp_path / "golden.json", broken)
    output = tmp_path / "output.json"
    write_metadata_file(output, _basic_1_as_mcps())
    with pytest.raises(pytest.fail.Exception, match="proposedSnapshot"):
        assert_metadata_files_equal(output, golden, golden_mces_as_mcps=True)


def test_update_golden_rewrites_mce_golden_as_mcps(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    golden_mce = {**basic_1[0], "systemMetadata": {"lastObserved": 1, "runId": "a"}}
    golden = _write_json(tmp_path / "golden.json", [golden_mce])
    output_mcps = _basic_1_as_mcps()
    memory = output_mcps[1].aspect
    assert isinstance(memory, InstitutionalMemoryClass)
    memory.elements[0].description = "Updated doc"
    for mcp in output_mcps:
        mcp.systemMetadata = SystemMetadataClass(lastObserved=2, runId="b")
    output = tmp_path / "output.json"
    write_metadata_file(output, output_mcps)

    monkeypatch.setattr(
        pytest_hooks,
        "_settings",
        pytest_hooks.GoldenFileSettings(update_golden=True, copy_output=False),
    )
    assert_metadata_files_equal(output, golden, golden_mces_as_mcps=True)

    # The untouched aspect keeps the golden's systemMetadata: the delta was applied,
    # not the output copied over.
    updated = json.loads(golden.read_text())
    assert [r["aspectName"] for r in updated] == ["ownership", "institutionalMemory"]
    assert updated[0]["systemMetadata"]["runId"] == "a"
    monkeypatch.undo()
    assert_metadata_files_equal(output, golden)
