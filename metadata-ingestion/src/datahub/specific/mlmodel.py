from typing import List, Optional, Tuple

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath
from datahub.metadata.schema_classes import (
    KafkaAuditHeaderClass,
    MLHyperParamClass as MLHyperParam,
    MLMetricClass as MLMetric,
    MLModelPropertiesClass as MLModelProperties,
    SystemMetadataClass,
    VersionTagClass as VersionTag,
)
from datahub.specific.aspect_helpers.custom_properties import HasCustomPropertiesPatch
from datahub.specific.aspect_helpers.domains import HasDomainsPatch
from datahub.specific.aspect_helpers.institutional_memory import (
    HasInstitutionalMemoryPatch,
)
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.specific.aspect_helpers.structured_properties import (
    HasStructuredPropertiesPatch,
)
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.specific.aspect_helpers.terms import HasTermsPatch


class MLModelPatchBuilder(
    HasOwnershipPatch,
    HasCustomPropertiesPatch,
    HasStructuredPropertiesPatch,
    HasTagsPatch,
    HasTermsPatch,
    HasDomainsPatch,
    HasInstitutionalMemoryPatch,
    MetadataPatchProposal,
):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn,
            system_metadata=system_metadata,
            audit_header=audit_header,
        )

    @classmethod
    def _custom_properties_location(cls) -> Tuple[str, PatchPath]:
        return MLModelProperties.ASPECT_NAME, ("customProperties",)

    def set_name(self, name: str) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("name",),
            value=name,
        )
        return self

    def set_description(self, description: str) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("description",),
            value=description,
        )
        return self

    def set_external_url(self, external_url: str) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("externalUrl",),
            value=external_url,
        )
        return self

    def set_type(self, type: str) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("type",),
            value=type,
        )
        return self

    def set_version(self, version: VersionTag) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("version",),
            value=version,
        )
        return self

    def set_hyper_params(
        self, hyper_params: List[MLHyperParam]
    ) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("hyperParams",),
            value=hyper_params,
        )
        return self

    def set_training_metrics(
        self, training_metrics: List[MLMetric]
    ) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("trainingMetrics",),
            value=training_metrics,
        )
        return self

    def set_ml_features(self, ml_features: List[str]) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("mlFeatures",),
            value=ml_features,
        )
        return self

    def add_ml_feature(self, feature_urn: str) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("mlFeatures", feature_urn),
            value=feature_urn,
        )
        return self

    def remove_ml_feature(self, feature_urn: str) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "remove",
            path=("mlFeatures", feature_urn),
            value={},
        )
        return self

    def set_training_jobs(self, training_jobs: List[str]) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("trainingJobs",),
            value=training_jobs,
        )
        return self

    def set_downstream_jobs(self, downstream_jobs: List[str]) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("downstreamJobs",),
            value=downstream_jobs,
        )
        return self

    def set_groups(self, groups: List[str]) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("groups",),
            value=groups,
        )
        return self

    def set_deployments(self, deployments: List[str]) -> "MLModelPatchBuilder":
        self._add_patch(
            MLModelProperties.ASPECT_NAME,
            "add",
            path=("deployments",),
            value=deployments,
        )
        return self
