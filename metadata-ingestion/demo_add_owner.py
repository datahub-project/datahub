import logging

from datahub.emitter.mce_builder import make_dataset_urn, make_user_urn
from datahub.emitter.mcp_patch_builder import DatasetPatchBuilder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import OwnerClass, OwnershipTypeClass

logging.basicConfig(level=logging.DEBUG)

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

dataset_urn = make_dataset_urn(platform="hive", name="SampleHiveDataset", env="PROD")
owner_to_add = OwnerClass(
    owner=make_user_urn("jdoe"), type=OwnershipTypeClass.DATAOWNER
)

DatasetPatchBuilder(dataset_urn).add_owner(owner_to_add).apply(emitter)
