import os

SHARED_ASPECTS_ENV_VAR = "SHARED_ASPECTS"
EXTRA_SHARED_ASPECTS_ENV_VAR = "EXTRA_SHARED_ASPECTS"

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
