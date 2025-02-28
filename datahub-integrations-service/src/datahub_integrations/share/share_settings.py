import os

SHARED_ASPECTS_ENV_VAR = "FEAT_SHARE_ASPECTS"
EXTRA_SHARED_ASPECTS_ENV_VAR = "FEAT_SHARE_EXTRA_ASPECTS"
SKIP_CACHE_ON_LINEAGE_QUERY_ENV_VAR = "FEAT_SHARE_SKIP_CACHE_ON_LINEAGE_QUERY"
REPORTING_HEARTBEAT_INTERVAL_ENV_VAR = "FEAT_SHARE_REPORTING_HEARTBEAT_INTERVAL_SECONDS"
MAX_ENTITIES_PER_SHARE_ENV_VAR = "FEAT_SHARE_MAX_ENTITIES_PER_SHARE"
PLATFORM_INSTANCE_ID_ENV_VAR = "FEAT_SHARE_PLATFORM_INSTANCE_ID"
PLATFORM_INSTANCE_NAME_ENV_VAR = "FEAT_SHARE_PLATFORM_INSTANCE_NAME"

# TODO: This hardcoded list of aspect names is not ideal - we're going to need
# something better here long-term.
_DEFAULT_SHARED_ASPECTS = {
    "editableDataJobProperties",
    "versionInfo",
    "dataFlowInfo",
    "editableDataFlowProperties",
    "dataJobInputOutput",
    "dataJobInfo",
    "dataProcessInstanceOutput",
    "dataProcessInstanceProperties",
    "dataProcessInfo",
    "dataProcessInstanceRelationships",
    "dataProcessInstanceRunEvent",
    "dataProcessInstanceInput",
    "viewProperties",
    "datasetUsageStatistics",
    "datasetUpstreamLineage",
    "datasetProperties",
    "editableDatasetProperties",
    "datasetDeprecation",
    "datasetProfile",
    "upstreamLineage",
    "chartQuery",
    "chartUsageStatistics",
    "chartInfo",
    "editableChartProperties",
    "dataPlatformInstanceProperties",
    "assertionInfo",
    "assertionRunEvent",
    "assertionDryRunEvent",
    "editableDashboardProperties",
    "dashboardUsageStatistics",
    "dashboardInfo",
    "containerProperties",
    "editableContainerProperties",
    "container",
    "editableSchemaMetadata",
    "schemaMetadata",
    "cost",
    "origin",
    "glossaryTerms",
    "siblings",
    "dataPlatformInstance",
    "institutionalMemory",
    "embed",
    "ownership",
    "status",
    "globalTags",
    "deprecation",
    "subTypes",
    "browsePathsV2",
    "operation",
    "inputFields",
    "browsePaths",
    "editableMlModelGroupProperties",
    "mlModelGroupProperties",
    "mlModelEthicalConsiderations",
    "mlFeatureProperties",
    "mlModelEvaluationData",
    "mlModelCaveatsAndRecommendations",
    "mlPrimaryKeyProperties",
    "editableMlPrimaryKeyProperties",
    "mlModelMetrics",
    "mlModelDeploymentProperties",
    "editableMlFeatureProperties",
    "sourceCode",
    "mlHyperParam",
    "editableMlFeatureTableProperties",
    "editableMlModelProperties",
    "mlModelTrainingData",
    "mlFeatureTableProperties",
    "intendedUse",
    "mlModelQuantitativeAnalyses",
    "mlMetric",
    "mlModelProperties",
    "mlModelFactorPrompts",
    "structuredProperties",
    "propertyDefinition",
    "dataPlatformInfo",
    "glossaryRelatedTerms",
    "glossaryTermInfo",
    "glossaryNodeInfo",
    "dataProductProperties",
    "editableNotebookProperties",
    "notebookContent",
    "notebookInfo",
    "querySubjects",
    "queryProperties",
    "domainProperties",
    "domains",
    "tagProperties",
    "dataContractProperties",
    "dataContractStatus",
}

RESTRICTED_SHARED_ASPECTS = {
    "dataJobInputOutput",
    "inputFields",
    "status",
    "upstreamLineage",
    "origin",
}


if SHARED_ASPECTS_ENV_VAR in os.environ:
    # Allow an override of the shared aspects via an environment variable.
    SHARED_ASPECTS = set(os.environ[SHARED_ASPECTS_ENV_VAR].split(","))
    assert "share" not in SHARED_ASPECTS, "Cannot share the share aspect"
else:
    SHARED_ASPECTS = _DEFAULT_SHARED_ASPECTS.copy()

if EXTRA_SHARED_ASPECTS_ENV_VAR in os.environ:
    # Allow to add extra shared aspects via an environment variable.
    EXTRA_SHARED_ASPECTS = set(os.environ[EXTRA_SHARED_ASPECTS_ENV_VAR].split(","))
    assert "share" not in EXTRA_SHARED_ASPECTS, "Cannot share the share aspect"
    SHARED_ASPECTS.update(EXTRA_SHARED_ASPECTS)

SKIP_CACHE_ON_LINEAGE_QUERY = (
    os.environ.get("FEAT_SHARE_SKIP_CACHE_ON_LINEAGE_QUERY", "false").lower() == "true"
)

REPORTING_HEARTBEAT_INTERVAL = int(
    os.getenv(REPORTING_HEARTBEAT_INTERVAL_ENV_VAR, "60")
)

MAX_ENTITIES_PER_SHARE = int(os.getenv(MAX_ENTITIES_PER_SHARE_ENV_VAR, "1000"))


_ACTOR_URN = "urn:li:corpuser:__integrations"

_PLATFORM_NAME = "acryl"

PLATFORM_INSTANCE_NAME = os.getenv(PLATFORM_INSTANCE_NAME_ENV_VAR)
