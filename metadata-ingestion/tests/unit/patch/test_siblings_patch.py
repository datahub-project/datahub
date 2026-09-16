import json

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.specific.dataset import DatasetPatchBuilder


def test_add_sibling_patch():
    """Test basic sibling addition with patch builder."""
    dataset_urn = make_dataset_urn(
        platform="hive", name="fct_users_created", env="PROD"
    )
    sibling_urn = make_dataset_urn(platform="dbt", name="test.model", env="PROD")

    patcher = DatasetPatchBuilder(dataset_urn).add_sibling(sibling_urn, primary=False)

    patches = patcher.build()
    assert len(patches) == 1

    patch = patches[0]
    assert patch.entityType == "dataset"
    assert patch.entityUrn == dataset_urn
    assert patch.changeType == "PATCH"
    assert patch.aspectName == "siblings"

    # Parse the patch content
    assert patch.aspect is not None
    patch_content = json.loads(patch.aspect.value.decode())
    assert len(patch_content) == 1

    patch_op = patch_content[0]
    assert patch_op["op"] == "add"
    assert patch_op["path"] == f"/siblings/{sibling_urn}"
    assert patch_op["value"] == sibling_urn


def test_add_sibling_patch_with_primary():
    """Test sibling addition with primary flag set to true."""
    dataset_urn = make_dataset_urn(
        platform="hive", name="fct_users_created", env="PROD"
    )
    sibling_urn = make_dataset_urn(platform="dbt", name="test.model", env="PROD")

    patcher = DatasetPatchBuilder(dataset_urn).add_sibling(sibling_urn, primary=True)

    patches = patcher.build()
    assert len(patches) == 1

    patch = patches[0]
    assert patch.aspectName == "siblings"

    # Parse the patch content
    assert patch.aspect is not None
    patch_content = json.loads(patch.aspect.value.decode())
    assert len(patch_content) == 2

    # Check sibling addition
    sibling_op = patch_content[0]
    assert sibling_op["op"] == "add"
    assert sibling_op["path"] == f"/siblings/{sibling_urn}"
    assert sibling_op["value"] == sibling_urn

    # Check primary flag
    primary_op = patch_content[1]
    assert primary_op["op"] == "add"
    assert primary_op["path"] == "/primary"
    assert primary_op["value"] is True


def test_remove_sibling_patch():
    """Test sibling removal with patch builder."""
    dataset_urn = make_dataset_urn(
        platform="hive", name="fct_users_created", env="PROD"
    )
    sibling_urn = make_dataset_urn(platform="dbt", name="test.model", env="PROD")

    patcher = DatasetPatchBuilder(dataset_urn).remove_sibling(sibling_urn)

    patches = patcher.build()
    assert len(patches) == 1

    patch = patches[0]
    assert patch.aspectName == "siblings"

    # Parse the patch content
    assert patch.aspect is not None
    patch_content = json.loads(patch.aspect.value.decode())
    assert len(patch_content) == 1

    patch_op = patch_content[0]
    assert patch_op["op"] == "remove"
    assert patch_op["path"] == f"/siblings/{sibling_urn}"
    assert patch_op["value"] == {}


def test_set_siblings_patch():
    """Test setting multiple siblings at once."""
    dataset_urn = make_dataset_urn(
        platform="hive", name="fct_users_created", env="PROD"
    )
    sibling_urns = [
        make_dataset_urn(platform="dbt", name="test.model1", env="PROD"),
        make_dataset_urn(platform="dbt", name="test.model2", env="PROD"),
    ]

    patcher = DatasetPatchBuilder(dataset_urn).set_siblings(sibling_urns, primary=True)

    patches = patcher.build()
    assert len(patches) == 1

    patch = patches[0]
    assert patch.aspectName == "siblings"

    # Parse the patch content
    assert patch.aspect is not None
    patch_content = json.loads(patch.aspect.value.decode())
    assert len(patch_content) == 2

    # Check siblings setting
    siblings_op = patch_content[0]
    assert siblings_op["op"] == "add"
    assert siblings_op["path"] == "/siblings"
    assert siblings_op["value"] == sibling_urns

    # Check primary flag
    primary_op = patch_content[1]
    assert primary_op["op"] == "add"
    assert primary_op["path"] == "/primary"
    assert primary_op["value"] is True


def test_multiple_sibling_operations():
    """Test multiple sibling operations in sequence."""
    dataset_urn = make_dataset_urn(
        platform="hive", name="fct_users_created", env="PROD"
    )
    sibling_urn1 = make_dataset_urn(platform="dbt", name="test.model1", env="PROD")
    sibling_urn2 = make_dataset_urn(platform="dbt", name="test.model2", env="PROD")
    sibling_urn3 = make_dataset_urn(platform="dbt", name="test.model3", env="PROD")

    patcher = (
        DatasetPatchBuilder(dataset_urn)
        .add_sibling(sibling_urn1, primary=False)
        .add_sibling(sibling_urn2, primary=True)
        .remove_sibling(sibling_urn3)
    )

    patches = patcher.build()
    assert len(patches) == 1

    patch = patches[0]
    assert patch.aspectName == "siblings"

    # Parse the patch content
    assert patch.aspect is not None
    patch_content = json.loads(patch.aspect.value.decode())
    assert (
        len(patch_content) == 4
    )  # add sibling1, add sibling2, set primary, remove sibling3

    # Verify operations
    operations = {op["path"]: op for op in patch_content}

    # Check first sibling addition (no primary)
    assert f"/siblings/{sibling_urn1}" in operations
    assert operations[f"/siblings/{sibling_urn1}"]["op"] == "add"
    assert operations[f"/siblings/{sibling_urn1}"]["value"] == sibling_urn1

    # Check second sibling addition (with primary)
    assert f"/siblings/{sibling_urn2}" in operations
    assert operations[f"/siblings/{sibling_urn2}"]["op"] == "add"
    assert operations[f"/siblings/{sibling_urn2}"]["value"] == sibling_urn2

    # Check primary flag
    assert "/primary" in operations
    assert operations["/primary"]["op"] == "add"
    assert operations["/primary"]["value"] is True

    # Check removal
    assert f"/siblings/{sibling_urn3}" in operations
    assert operations[f"/siblings/{sibling_urn3}"]["op"] == "remove"


def test_sibling_patch_builder_inheritance():
    """Test that DatasetPatchBuilder properly inherits sibling functionality."""
    dataset_urn = make_dataset_urn(
        platform="hive", name="fct_users_created", env="PROD"
    )
    builder = DatasetPatchBuilder(dataset_urn)

    # Verify that the builder has sibling methods
    assert hasattr(builder, "add_sibling")
    assert hasattr(builder, "remove_sibling")
    assert hasattr(builder, "set_siblings")

    # Verify method chaining works
    result = builder.add_sibling("urn:li:dataset:test", primary=True)
    assert result is builder  # Should return self for chaining


def test_sibling_patch_with_complex_urns():
    """Test sibling patches work with complex URNs containing special characters."""
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.schema.table_name,PROD)"
    sibling_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,project.model_name,DEV)"

    patcher = DatasetPatchBuilder(dataset_urn).add_sibling(sibling_urn, primary=False)

    patches = patcher.build()
    assert len(patches) == 1

    patch = patches[0]
    assert patch.aspect is not None
    patch_content = json.loads(patch.aspect.value.decode())

    patch_op = patch_content[0]
    assert patch_op["path"] == f"/siblings/{sibling_urn}"
    assert patch_op["value"] == sibling_urn
