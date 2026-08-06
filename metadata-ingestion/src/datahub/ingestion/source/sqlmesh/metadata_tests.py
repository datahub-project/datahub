import hashlib
import json
import logging
from typing import Any, Dict, Iterable, List

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sqlmesh.base import SqlmeshSourceBase
from datahub.ingestion.source.sqlmesh.constants import (
    SQLMESH_DISPLAY_NAME,
    SQLMESH_PLATFORM,
)
from datahub.ingestion.source.sqlmesh.models import _MetadataTestSpec
from datahub.metadata.schema_classes import (
    TestDefinitionClass,
    TestDefinitionTypeClass,
    TestInfoClass,
)

logger = logging.getLogger(__name__)


class MetadataTestMixin(SqlmeshSourceBase):
    def _emit_metadata_tests(self) -> Iterable[MetadataWorkUnit]:
        """Emit governance Metadata Test entities scoped to this project's models.

        The Test entity is part of the core metadata model, so any DataHub
        instance accepts and stores these definitions; evaluating them requires
        a deployment with a Metadata Tests runner (DataHub Cloud). The test URN
        is derived from the platform/instance scope so re-ingestion is
        idempotent and two projects with distinct ``sqlmesh_platform_instance``
        values get distinct tests.
        """
        platform_urn = make_data_platform_urn(SQLMESH_PLATFORM)
        conditions: List[Dict[str, Any]] = [
            {
                "property": "dataPlatformInstance.platform",
                "operator": "equals",
                "value": platform_urn,
            }
        ]
        scope_key = platform_urn
        scope_label = SQLMESH_DISPLAY_NAME
        if self.config.sqlmesh_platform_instance:
            instance_urn = make_dataplatform_instance_urn(
                SQLMESH_PLATFORM, self.config.sqlmesh_platform_instance
            )
            conditions.append(
                {
                    "property": "dataPlatformInstance.instance",
                    "operator": "equals",
                    "value": instance_urn,
                }
            )
            scope_key = instance_urn
            scope_label = (
                f"{SQLMESH_DISPLAY_NAME} ({self.config.sqlmesh_platform_instance})"
            )

        tests = [
            _MetadataTestSpec(
                suffix="documentation",
                name=f"{scope_label}: models have documentation",
                description=(
                    "Every SQLMesh model in this project should carry a description, "
                    "either from the model definition or added in DataHub."
                ),
                rules={
                    "or": [
                        {
                            "property": "datasetProperties.description",
                            "operator": "exists",
                        },
                        {
                            "property": "editableDatasetProperties.description",
                            "operator": "exists",
                        },
                    ]
                },
            ),
            _MetadataTestSpec(
                suffix="ownership",
                name=f"{scope_label}: models have owners",
                description=(
                    "Every SQLMesh model in this project should have an owner, "
                    "either from the model's owner field or assigned in DataHub."
                ),
                rules={
                    "and": [
                        {"property": "ownership.owners.owner", "operator": "exists"}
                    ]
                },
            ),
        ]
        scope_hash = hashlib.md5(scope_key.encode("utf-8")).hexdigest()[:12]
        for test in tests:
            definition = {
                "on": {"types": ["dataset"], "conditions": {"and": conditions}},
                "rules": test.rules,
            }
            yield MetadataChangeProposalWrapper(
                entityUrn=f"urn:li:test:sqlmesh-{scope_hash}-{test.suffix}",
                aspect=TestInfoClass(
                    name=test.name,
                    category=SQLMESH_DISPLAY_NAME,
                    description=test.description,
                    definition=TestDefinitionClass(
                        type=TestDefinitionTypeClass.JSON,
                        json=json.dumps(definition, indent=2),
                    ),
                ),
            ).as_workunit()
