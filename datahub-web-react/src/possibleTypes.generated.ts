
      export interface PossibleTypesResultData {
        possibleTypes: {
          [key: string]: string[]
        }
      }
      const result: PossibleTypesResultData = {
  "possibleTypes": {
    "AnalyticsChart": [
      "BarChart",
      "TableChart",
      "TimeSeriesChart"
    ],
    "Aspect": [
      "SchemaMetadata"
    ],
    "BrowsableEntity": [
      "Chart",
      "Dashboard",
      "DataFlow",
      "DataJob",
      "Dataset",
      "MLFeatureTable",
      "MLModel",
      "MLModelGroup",
      "Notebook"
    ],
    "Entity": [
      "AccessTokenMetadata",
      "Application",
      "Assertion",
      "BusinessAttribute",
      "Chart",
      "Container",
      "CorpGroup",
      "CorpUser",
      "Dashboard",
      "DataContract",
      "DataFlow",
      "DataHubConnection",
      "DataHubFile",
      "DataHubPageModule",
      "DataHubPageTemplate",
      "DataHubPolicy",
      "DataHubRole",
      "DataHubView",
      "DataJob",
      "DataPlatform",
      "DataPlatformInstance",
      "DataProcessInstance",
      "DataProduct",
      "DataTypeEntity",
      "Dataset",
      "Document",
      "Domain",
      "ERModelRelationship",
      "EntityTypeEntity",
      "ExecutionRequest",
      "Form",
      "GlossaryNode",
      "GlossaryTerm",
      "Incident",
      "MLFeature",
      "MLFeatureTable",
      "MLModel",
      "MLModelGroup",
      "MLPrimaryKey",
      "Notebook",
      "OwnershipTypeEntity",
      "Post",
      "QueryEntity",
      "Restricted",
      "Role",
      "SchemaFieldEntity",
      "ServiceAccount",
      "StructuredPropertyEntity",
      "Tag",
      "Test",
      "VersionSet",
      "VersionedDataset"
    ],
    "EntityWithRelationships": [
      "Assertion",
      "Chart",
      "Dashboard",
      "DataFlow",
      "DataJob",
      "DataProcessInstance",
      "Dataset",
      "ERModelRelationship",
      "MLFeature",
      "MLFeatureTable",
      "MLModel",
      "MLModelGroup",
      "MLPrimaryKey",
      "Restricted",
      "SchemaFieldEntity"
    ],
    "HasExecutionRuns": [
      "DataFlow",
      "DataJob"
    ],
    "HasLogicalParent": [
      "Dataset",
      "SchemaFieldEntity"
    ],
    "HyperParameterValueType": [
      "BooleanBox",
      "FloatBox",
      "IntBox",
      "StringBox"
    ],
    "OwnerType": [
      "CorpGroup",
      "CorpUser"
    ],
    "PlatformSchema": [
      "KeyValueSchema",
      "TableSchema"
    ],
    "PropertyValue": [
      "NumberValue",
      "StringValue"
    ],
    "ResolvedActor": [
      "CorpGroup",
      "CorpUser"
    ],
    "ResultsType": [
      "StringBox"
    ],
    "SupportsVersions": [
      "Dataset",
      "MLModel"
    ],
    "TimeSeriesAspect": [
      "AssertionRunEvent",
      "DashboardUsageMetrics",
      "DataProcessRunEvent",
      "DatasetProfile",
      "Operation"
    ]
  }
};
      export default result;
    