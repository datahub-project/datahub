package com.linkedin.metadata;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.Arrays;
import java.util.List;

/** Static class containing commonly-used constants across DataHub services. */
public class Constants {
  public static final String INTERNAL_DELEGATED_FOR_ACTOR_HEADER_NAME = "X-DataHub-Delegated-For";
  public static final String INTERNAL_DELEGATED_FOR_ACTOR_TYPE = "X-DataHub-Delegated-For-";

  public static final String URN_LI_PREFIX = "urn:li:";
  public static final String DATAHUB_ACTOR = "urn:li:corpuser:datahub"; // Super user.
  public static final String SYSTEM_ACTOR =
      "urn:li:corpuser:__datahub_system"; // DataHub internal service principal.
  public static final String UNKNOWN_ACTOR = "urn:li:corpuser:UNKNOWN"; // Unknown principal.
  public static final Long ASPECT_LATEST_VERSION = 0L;
  public static final String UNKNOWN_DATA_PLATFORM = "urn:li:dataPlatform:unknown";
  public static final String ENTITY_TYPE_URN_PREFIX = "urn:li:entityType:";
  public static final String DATA_TYPE_URN_PREFIX = "urn:li:dataType:";
  public static final String STRUCTURED_PROPERTY_MAPPING_FIELD = "structuredProperties";
  public static final String STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX =
      STRUCTURED_PROPERTY_MAPPING_FIELD + ".";
  public static final String STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD = "_versioned";
  public static final String STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD_PREFIX =
      String.join(
          ".", STRUCTURED_PROPERTY_MAPPING_FIELD, STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD, "");

  // !!!!!!! IMPORTANT !!!!!!!
  // This effectively sets the max aspect size to 16 MB. Used in deserialization
  // of messages.
  // Without this the limit is
  // whatever Jackson is defaulting to (5 MB currently).
  public static final String MAX_JACKSON_STRING_SIZE = "16000000";
  public static final String INGESTION_MAX_SERIALIZED_STRING_LENGTH =
      "INGESTION_MAX_SERIALIZED_STRING_LENGTH";

  /** System Metadata */
  public static final String DEFAULT_RUN_ID = "no-run-id-provided";

  // Forces indexing for no-ops, enabled for restore indices calls. Only
  // considered in the no-op
  // case
  public static final String FORCE_INDEXING_KEY = "forceIndexing";
  // Indicates an event source from an application with hooks that have already
  // been processed and
  // should not be reprocessed
  public static final String APP_SOURCE = "appSource";

  // App sources
  public static final String UI_SOURCE = "ui";
  public static final String SYSTEM_UPDATE_SOURCE = "systemUpdate";
  public static final String METADATA_TESTS_SOURCE = "metadataTests";

  /** Entities */
  public static final String CORP_USER_ENTITY_NAME = "corpuser";

  public static final String CORP_GROUP_ENTITY_NAME = "corpGroup";
  public static final String DATASET_ENTITY_NAME = "dataset";
  public static final String CHART_ENTITY_NAME = "chart";
  public static final String DASHBOARD_ENTITY_NAME = "dashboard";
  public static final String DATA_FLOW_ENTITY_NAME = "dataFlow";
  public static final String DATA_JOB_ENTITY_NAME = "dataJob";
  public static final String DATA_PLATFORM_ENTITY_NAME = "dataPlatform";
  public static final String GLOSSARY_TERM_ENTITY_NAME = "glossaryTerm";
  public static final String GLOSSARY_NODE_ENTITY_NAME = "glossaryNode";
  public static final String ML_FEATURE_ENTITY_NAME = "mlFeature";
  public static final String ML_FEATURE_TABLE_ENTITY_NAME = "mlFeatureTable";
  public static final String ML_MODEL_ENTITY_NAME = "mlModel";
  public static final String ML_MODEL_GROUP_ENTITY_NAME = "mlModelGroup";
  public static final String ML_PRIMARY_KEY_ENTITY_NAME = "mlPrimaryKey";
  public static final String POLICY_ENTITY_NAME = "dataHubPolicy";
  public static final String TAG_ENTITY_NAME = "tag";
  public static final String CONTAINER_ENTITY_NAME = "container";
  public static final String DOMAIN_ENTITY_NAME = "domain";
  public static final String ER_MODEL_RELATIONSHIP_ENTITY_NAME = "erModelRelationship";
  public static final String ASSERTION_ENTITY_NAME = "assertion";
  public static final String INCIDENT_ENTITY_NAME = "incident";
  public static final String INGESTION_SOURCE_ENTITY_NAME = "dataHubIngestionSource";
  public static final String SECRETS_ENTITY_NAME = "dataHubSecret";
  public static final String EXECUTION_REQUEST_ENTITY_NAME = "dataHubExecutionRequest";
  public static final String NOTEBOOK_ENTITY_NAME = "notebook";
  public static final String DATA_PLATFORM_INSTANCE_ENTITY_NAME = "dataPlatformInstance";
  public static final String ACCESS_TOKEN_ENTITY_NAME = "dataHubAccessToken";
  public static final String DATA_HUB_UPGRADE_ENTITY_NAME = "dataHubUpgrade";
  public static final String INVITE_TOKEN_ENTITY_NAME = "inviteToken";
  public static final String DATAHUB_ROLE_ENTITY_NAME = "dataHubRole";
  public static final String POST_ENTITY_NAME = "post";
  public static final String SCHEMA_FIELD_ENTITY_NAME = "schemaField";
  public static final String SCHEMA_FIELD_KEY_ASPECT = "schemaFieldKey";
  public static final String SCHEMA_FIELD_ALIASES_ASPECT = "schemaFieldAliases";
  public static final String DATAHUB_STEP_STATE_ENTITY_NAME = "dataHubStepState";
  public static final String DATAHUB_VIEW_ENTITY_NAME = "dataHubView";
  public static final String QUERY_ENTITY_NAME = "query";
  public static final String DATA_PRODUCT_ENTITY_NAME = "dataProduct";
  public static final String OWNERSHIP_TYPE_ENTITY_NAME = "ownershipType";
  public static final Urn DEFAULT_OWNERSHIP_TYPE_URN =
      UrnUtils.getUrn("urn:li:ownershipType:__system__none");
  public static final String STRUCTURED_PROPERTY_ENTITY_NAME = "structuredProperty";
  public static final String DATA_TYPE_ENTITY_NAME = "dataType";
  public static final String ENTITY_TYPE_ENTITY_NAME = "entityType";
  public static final String FORM_ENTITY_NAME = "form";
  public static final String RESTRICTED_ENTITY_NAME = "restricted";
  public static final String BUSINESS_ATTRIBUTE_ENTITY_NAME = "businessAttribute";

  /** Aspects */
  // Common
  public static final String OWNERSHIP_ASPECT_NAME = "ownership";

  public static final String TIMESTAMP_MILLIS = "timestampMillis";

  public static final String INSTITUTIONAL_MEMORY_ASPECT_NAME = "institutionalMemory";
  public static final String DATA_PLATFORM_INSTANCE_ASPECT_NAME = "dataPlatformInstance";
  public static final String BROWSE_PATHS_ASPECT_NAME = "browsePaths";
  public static final String BROWSE_PATHS_V2_ASPECT_NAME = "browsePathsV2";
  public static final String GLOBAL_TAGS_ASPECT_NAME = "globalTags";
  public static final String GLOSSARY_TERMS_ASPECT_NAME = "glossaryTerms";
  public static final String STATUS_ASPECT_NAME = "status";
  public static final String SUB_TYPES_ASPECT_NAME = "subTypes";
  public static final String DEPRECATION_ASPECT_NAME = "deprecation";
  public static final String OPERATION_ASPECT_NAME = "operation";
  public static final String OPERATION_EVENT_TIME_FIELD_NAME = "lastUpdatedTimestamp"; // :(
  public static final String SIBLINGS_ASPECT_NAME = "siblings";
  public static final String ORIGIN_ASPECT_NAME = "origin";
  public static final String INPUT_FIELDS_ASPECT_NAME = "inputFields";
  public static final String EMBED_ASPECT_NAME = "embed";
  public static final String INCIDENTS_SUMMARY_ASPECT_NAME = "incidentsSummary";
  public static final String DOCUMENTATION_ASPECT_NAME = "documentation";
  public static final String DATA_TRANSFORM_LOGIC_ASPECT_NAME = "dataTransformLogic";
  public static final String VERSION_PROPERTIES_ASPECT_NAME = "versionProperties";

  // User
  public static final String CORP_USER_KEY_ASPECT_NAME = "corpUserKey";
  public static final String CORP_USER_EDITABLE_INFO_NAME = "corpUserEditableInfo";
  public static final String GROUP_MEMBERSHIP_ASPECT_NAME = "groupMembership";
  public static final String NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME = "nativeGroupMembership";
  public static final String CORP_USER_EDITABLE_INFO_ASPECT_NAME = "corpUserEditableInfo";
  public static final String CORP_USER_INFO_ASPECT_NAME = "corpUserInfo";
  public static final String CORP_USER_STATUS_ASPECT_NAME = "corpUserStatus";
  public static final String CORP_USER_CREDENTIALS_ASPECT_NAME = "corpUserCredentials";
  public static final String ROLE_MEMBERSHIP_ASPECT_NAME = "roleMembership";

  public static final String CORP_USER_SETTINGS_ASPECT_NAME = "corpUserSettings";
  public static final String CORP_USER_STATUS_LAST_MODIFIED_FIELD_NAME = "statusLastModifiedAt";

  // Group
  public static final String CORP_GROUP_KEY_ASPECT_NAME = "corpGroupKey";
  public static final String CORP_GROUP_INFO_ASPECT_NAME = "corpGroupInfo";
  public static final String CORP_GROUP_EDITABLE_INFO_ASPECT_NAME = "corpGroupEditableInfo";
  public static final String CORP_GROUP_CREATED_TIME_INDEX_FIELD_NAME = "createdTime";

  // Dataset
  public static final String DATASET_KEY_ASPECT_NAME = "datasetKey";
  public static final String DATASET_PROPERTIES_ASPECT_NAME = "datasetProperties";
  public static final String EDITABLE_DATASET_PROPERTIES_ASPECT_NAME = "editableDatasetProperties";
  public static final String DATASET_DEPRECATION_ASPECT_NAME = "datasetDeprecation";
  public static final String DATASET_UPSTREAM_LINEAGE_ASPECT_NAME = "datasetUpstreamLineage";
  public static final String UPSTREAM_LINEAGE_ASPECT_NAME = "upstreamLineage";
  public static final String SCHEMA_METADATA_ASPECT_NAME = "schemaMetadata";
  public static final String EDITABLE_SCHEMA_METADATA_ASPECT_NAME = "editableSchemaMetadata";
  public static final String VIEW_PROPERTIES_ASPECT_NAME = "viewProperties";
  public static final String DATASET_PROFILE_ASPECT_NAME = "datasetProfile";

  public static final String STRUCTURED_PROPERTIES_ASPECT_NAME = "structuredProperties";
  public static final String FORMS_ASPECT_NAME = "forms";
  // Aspect support
  public static final String FINE_GRAINED_LINEAGE_DATASET_TYPE = "DATASET";
  public static final String FINE_GRAINED_LINEAGE_FIELD_SET_TYPE = "FIELD_SET";
  public static final String FINE_GRAINED_LINEAGE_FIELD_TYPE = "FIELD";

  // Chart
  public static final String CHART_KEY_ASPECT_NAME = "chartKey";
  public static final String CHART_INFO_ASPECT_NAME = "chartInfo";
  public static final String EDITABLE_CHART_PROPERTIES_ASPECT_NAME = "editableChartProperties";
  public static final String CHART_QUERY_ASPECT_NAME = "chartQuery";
  public static final String CHART_USAGE_STATISTICS_ASPECT_NAME = "chartUsageStatistics";

  // Dashboard
  public static final String DASHBOARD_KEY_ASPECT_NAME = "dashboardKey";
  public static final String DASHBOARD_INFO_ASPECT_NAME = "dashboardInfo";
  public static final String EDITABLE_DASHBOARD_PROPERTIES_ASPECT_NAME =
      "editableDashboardProperties";
  public static final String DASHBOARD_USAGE_STATISTICS_ASPECT_NAME = "dashboardUsageStatistics";

  // Notebook
  public static final String NOTEBOOK_KEY_ASPECT_NAME = "notebookKey";
  public static final String NOTEBOOK_INFO_ASPECT_NAME = "notebookInfo";
  public static final String NOTEBOOK_CONTENT_ASPECT_NAME = "notebookContent";
  public static final String EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME =
      "editableNotebookProperties";

  // DataFlow
  public static final String DATA_FLOW_KEY_ASPECT_NAME = "dataFlowKey";
  public static final String DATA_FLOW_INFO_ASPECT_NAME = "dataFlowInfo";
  public static final String EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME =
      "editableDataFlowProperties";

  // DataJob
  public static final String DATA_JOB_KEY_ASPECT_NAME = "dataJobKey";
  public static final String DATA_JOB_INFO_ASPECT_NAME = "dataJobInfo";
  public static final String DATA_JOB_INPUT_OUTPUT_ASPECT_NAME = "dataJobInputOutput";
  public static final String EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME = "editableDataJobProperties";

  // DataPlatform
  public static final String DATA_PLATFORM_KEY_ASPECT_NAME = "dataPlatformKey";
  public static final String DATA_PLATFORM_INFO_ASPECT_NAME = "dataPlatformInfo";

  // DataPlatformInstance
  public static final String DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME = "dataPlatformInstanceKey";
  public static final String DATA_PLATFORM_INSTANCE_PROPERTIES_ASPECT_NAME =
      "dataPlatformInstanceProperties";

  // ML Feature
  public static final String ML_FEATURE_KEY_ASPECT_NAME = "mlFeatureKey";
  public static final String ML_FEATURE_PROPERTIES_ASPECT_NAME = "mlFeatureProperties";
  public static final String ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME =
      "editableMlFeatureProperties";

  // ML Feature Table
  public static final String ML_FEATURE_TABLE_KEY_ASPECT_NAME = "mlFeatureTableKey";
  public static final String ML_FEATURE_TABLE_PROPERTIES_ASPECT_NAME = "mlFeatureTableProperties";
  public static final String ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME =
      "editableMlFeatureTableProperties";

  // ML Model
  public static final String ML_MODEL_KEY_ASPECT_NAME = "mlModelKey";
  public static final String ML_MODEL_PROPERTIES_ASPECT_NAME = "mlModelProperties";
  public static final String ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME = "editableMlModelProperties";
  public static final String INTENDED_USE_ASPECT_NAME = "intendedUse";
  public static final String ML_MODEL_FACTOR_PROMPTS_ASPECT_NAME = "mlModelFactorPrompts";
  public static final String METRICS_ASPECT_NAME = "metrics";
  public static final String EVALUATION_DATA_ASPECT_NAME = "evaluationData";
  public static final String TRAINING_DATA_ASPECT_NAME = "trainingData";
  public static final String QUANTITATIVE_ANALYSES_ASPECT_NAME = "quantitativeAnalyses";
  public static final String ETHICAL_CONSIDERATIONS_ASPECT_NAME = "ethicalConsiderations";
  public static final String CAVEATS_AND_RECOMMENDATIONS_ASPECT_NAME = "caveatsAndRecommendations";
  public static final String SOURCE_CODE_ASPECT_NAME = "sourceCode";
  public static final String COST_ASPECT_NAME = "cost";

  // ML Model Group
  public static final String ML_MODEL_GROUP_KEY_ASPECT_NAME = "mlModelGroupKey";
  public static final String ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME = "mlModelGroupProperties";
  public static final String ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME =
      "editableMlModelGroupProperties";

  // ML Primary Key
  public static final String ML_PRIMARY_KEY_KEY_ASPECT_NAME = "mlPrimaryKeyKey";
  public static final String ML_PRIMARY_KEY_PROPERTIES_ASPECT_NAME = "mlPrimaryKeyProperties";
  public static final String ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME =
      "editableMlPrimaryKeyProperties";

  // Policy
  public static final String DATAHUB_POLICY_INFO_ASPECT_NAME = "dataHubPolicyInfo";

  // Role
  public static final String DATAHUB_ROLE_INFO_ASPECT_NAME = "dataHubRoleInfo";

  // Tag
  public static final String TAG_KEY_ASPECT_NAME = "tagKey";
  public static final String TAG_PROPERTIES_ASPECT_NAME = "tagProperties";

  // Container
  public static final String CONTAINER_KEY_ASPECT_NAME = "containerKey";
  public static final String CONTAINER_PROPERTIES_ASPECT_NAME = "containerProperties";
  public static final String CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME =
      "editableContainerProperties";
  public static final String CONTAINER_ASPECT_NAME = "container"; // parent container

  // Glossary term
  public static final String GLOSSARY_TERM_KEY_ASPECT_NAME = "glossaryTermKey";
  public static final String GLOSSARY_TERM_INFO_ASPECT_NAME = "glossaryTermInfo";
  public static final String GLOSSARY_RELATED_TERM_ASPECT_NAME = "glossaryRelatedTerms";

  // Glossary node
  public static final String GLOSSARY_NODE_KEY_ASPECT_NAME = "glossaryNodeKey";
  public static final String GLOSSARY_NODE_INFO_ASPECT_NAME = "glossaryNodeInfo";

  // Domain
  public static final String DOMAIN_KEY_ASPECT_NAME = "domainKey";
  public static final String DOMAIN_PROPERTIES_ASPECT_NAME = "domainProperties";
  public static final String DOMAINS_ASPECT_NAME = "domains";

  // ExternalRoleMetadata
  public static final String ROLE_ENTITY_NAME = "role";
  public static final String ACCESS_ASPECT_NAME = "access";
  public static final String ROLE_KEY = "roleKey";
  public static final String ROLE_PROPERTIES_ASPECT_NAME = "roleProperties";
  public static final String ROLE_ACTORS_ASPECT_NAME = "actors";

  public static final String DOMAIN_CREATED_TIME_INDEX_FIELD_NAME = "createdTime";

  // ERModelRelationship
  public static final String ER_MODEL_RELATIONSHIP_KEY_ASPECT_NAME = "erModelRelationshipKey";
  public static final String ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME =
      "erModelRelationshipProperties";
  public static final String EDITABLE_ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME =
      "editableERModelRelationshipProperties";

  // Assertion
  public static final String ASSERTION_KEY_ASPECT_NAME = "assertionKey";
  public static final String ASSERTION_INFO_ASPECT_NAME = "assertionInfo";
  public static final String ASSERTION_RUN_EVENT_ASPECT_NAME = "assertionRunEvent";
  public static final String ASSERTION_RUN_EVENT_STATUS_COMPLETE = "COMPLETE";
  public static final String ASSERTION_ACTIONS_ASPECT_NAME = "assertionActions";

  // Tests
  public static final String TEST_ENTITY_NAME = "test";
  public static final String TEST_KEY_ASPECT_NAME = "testKey";
  public static final String TEST_INFO_ASPECT_NAME = "testInfo";
  public static final String TEST_RESULTS_ASPECT_NAME = "testResults";

  // Incident
  public static final String INCIDENT_KEY_ASPECT_NAME = "incidentKey";
  public static final String INCIDENT_INFO_ASPECT_NAME = "incidentInfo";

  // DataHub Ingestion Source
  public static final String INGESTION_SOURCE_KEY_ASPECT_NAME = "dataHubIngestionSourceKey";
  public static final String INGESTION_INFO_ASPECT_NAME = "dataHubIngestionSourceInfo";

  // DataHub Secret
  public static final String SECRET_VALUE_ASPECT_NAME = "dataHubSecretValue";

  // DataHub Execution Request
  public static final String EXECUTION_REQUEST_INPUT_ASPECT_NAME = "dataHubExecutionRequestInput";
  public static final String EXECUTION_REQUEST_SIGNAL_ASPECT_NAME = "dataHubExecutionRequestSignal";
  public static final String EXECUTION_REQUEST_RESULT_ASPECT_NAME = "dataHubExecutionRequestResult";
  public static final String EXECUTION_REQUEST_STATUS_RUNNING = "RUNNING";
  public static final String EXECUTION_REQUEST_STATUS_FAILURE = "FAILURE";
  public static final String EXECUTION_REQUEST_STATUS_SUCCESS = "SUCCESS";
  public static final String EXECUTION_REQUEST_STATUS_TIMEOUT = "TIMEOUT";
  public static final String EXECUTION_REQUEST_STATUS_CANCELLED = "CANCELLED";
  public static final String EXECUTION_REQUEST_STATUS_ABORTED = "ABORTED";
  public static final String EXECUTION_REQUEST_STATUS_DUPLICATE = "DUPLICATE";

  // DataHub Access Token
  public static final String ACCESS_TOKEN_KEY_ASPECT_NAME = "dataHubAccessTokenKey";
  public static final String ACCESS_TOKEN_INFO_NAME = "dataHubAccessTokenInfo";

  // DataHub Upgrade
  public static final String DATA_HUB_UPGRADE_KEY_ASPECT_NAME = "dataHubUpgradeKey";
  public static final String DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME = "dataHubUpgradeRequest";
  public static final String DATA_HUB_UPGRADE_RESULT_ASPECT_NAME = "dataHubUpgradeResult";

  // Invite Token
  public static final String INVITE_TOKEN_ASPECT_NAME = "inviteToken";
  public static final int INVITE_TOKEN_LENGTH = 32;
  public static final int SALT_TOKEN_LENGTH = 16;
  public static final int PASSWORD_RESET_TOKEN_LENGTH = 32;

  // Views
  public static final String DATAHUB_VIEW_KEY_ASPECT_NAME = "dataHubViewKey";
  public static final String DATAHUB_VIEW_INFO_ASPECT_NAME = "dataHubViewInfo";

  // Query
  public static final String QUERY_PROPERTIES_ASPECT_NAME = "queryProperties";
  public static final String QUERY_SUBJECTS_ASPECT_NAME = "querySubjects";

  // DataProduct
  public static final String DATA_PRODUCT_PROPERTIES_ASPECT_NAME = "dataProductProperties";
  public static final String DATA_PRODUCTS_ASPECT_NAME = "dataProducts";

  // Ownership Types
  public static final String OWNERSHIP_TYPE_KEY_ASPECT_NAME = "ownershipTypeKey";
  public static final String OWNERSHIP_TYPE_INFO_ASPECT_NAME = "ownershipTypeInfo";

  // Structured Property
  public static final String STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME = "propertyDefinition";
  public static final String STRUCTURED_PROPERTY_KEY_ASPECT_NAME = "structuredPropertyKey";
  public static final String STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME =
      "structuredPropertySettings";

  // Form
  public static final String FORM_INFO_ASPECT_NAME = "formInfo";
  public static final String FORM_KEY_ASPECT_NAME = "formKey";
  public static final String DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME = "dynamicFormAssignment";

  // Data Type
  public static final String DATA_TYPE_INFO_ASPECT_NAME = "dataTypeInfo";

  // Entity Type
  public static final String ENTITY_TYPE_INFO_ASPECT_NAME = "entityTypeInfo";

  // Settings
  public static final String GLOBAL_SETTINGS_ENTITY_NAME = "globalSettings";
  public static final String GLOBAL_SETTINGS_INFO_ASPECT_NAME = "globalSettingsInfo";
  public static final Urn GLOBAL_SETTINGS_URN = Urn.createFromTuple(GLOBAL_SETTINGS_ENTITY_NAME, 0);

  // Connection
  public static final String DATAHUB_CONNECTION_ENTITY_NAME = "dataHubConnection";
  public static final String DATAHUB_CONNECTION_DETAILS_ASPECT_NAME = "dataHubConnectionDetails";

  // Data Contracts
  public static final String DATA_CONTRACT_ENTITY_NAME = "dataContract";
  public static final String DATA_CONTRACT_PROPERTIES_ASPECT_NAME = "dataContractProperties";
  public static final String DATA_CONTRACT_KEY_ASPECT_NAME = "dataContractKey";
  public static final String DATA_CONTRACT_STATUS_ASPECT_NAME = "dataContractStatus";

  // Relationships
  public static final String IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME = "IsMemberOfGroup";
  public static final String IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME = "IsMemberOfNativeGroup";

  public static final String CHANGE_EVENT_PLATFORM_EVENT_NAME = "entityChangeEvent";

  /** Retention */
  public static final String DATAHUB_RETENTION_ENTITY = "dataHubRetention";

  public static final String DATAHUB_RETENTION_ASPECT = "dataHubRetentionConfig";
  public static final String DATAHUB_RETENTION_KEY_ASPECT = "dataHubRetentionKey";

  /** User Status */
  public static final String CORP_USER_STATUS_ACTIVE = "ACTIVE";

  public static final String CORP_USER_STATUS_SUSPENDED = "SUSPENDED";

  /** Task Runs */
  public static final String DATA_PROCESS_INSTANCE_ENTITY_NAME = "dataProcessInstance";

  public static final String DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME =
      "dataProcessInstanceProperties";
  public static final String DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME =
      "dataProcessInstanceRunEvent";
  public static final String DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME =
      "dataProcessInstanceRelationships";
  public static final String DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME = "dataProcessInstanceInput";
  public static final String DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME = "dataProcessInstanceOutput";
  public static final String DATA_PROCESS_INSTANCE_KEY_ASPECT_NAME = "dataProcessInstanceKey";
  public static final String ML_TRAINING_RUN_PROPERTIES_ASPECT_NAME = "mlTrainingRunProperties";

  // Business Attribute
  public static final String BUSINESS_ATTRIBUTE_KEY_ASPECT_NAME = "businessAttributeKey";
  public static final String BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME = "businessAttributeInfo";
  public static final String BUSINESS_ATTRIBUTE_ASSOCIATION = "businessAttributeAssociation";
  public static final String BUSINESS_ATTRIBUTE_ASPECT = "businessAttributes";
  public static final List<String> SKIP_REFERENCE_ASPECT =
      Arrays.asList("ownership", "status", "institutionalMemory");

  // Posts
  public static final String POST_INFO_ASPECT_NAME = "postInfo";
  public static final String LAST_MODIFIED_FIELD_NAME = "lastModified";

  // Telemetry
  public static final String CLIENT_ID_URN = "urn:li:telemetry:clientId";
  public static final String CLIENT_ID_ASPECT = "telemetryClientId";

  // Step
  public static final String DATAHUB_STEP_STATE_PROPERTIES_ASPECT_NAME =
      "dataHubStepStateProperties";

  // Authorization
  public static final String REST_API_AUTHORIZATION_ENABLED_ENV = "REST_API_AUTHORIZATION_ENABLED";

  // Metadata Change Event Parameter Names

  // Runs
  public static final String RUN_RESULT_KEY = "runResult";
  public static final String RUN_ID_KEY = "runId";
  public static final String ASSERTEE_URN_KEY = "asserteeUrn";
  public static final String ASSERTION_RESULT_KEY = "assertionResult";
  public static final String ATTEMPT_KEY = "attempt";
  public static final String PARENT_INSTANCE_URN_KEY = "parentInstanceUrn";
  public static final String DATA_FLOW_URN_KEY = "dataFlowUrn";
  public static final String DATA_JOB_URN_KEY = "dataJobUrn";

  // Incidents
  public static final String ENTITY_REF = "entities";

  // Version Set
  public static final String VERSION_SET_ENTITY_NAME = "versionSet";
  public static final String VERSION_SET_KEY_ASPECT_NAME = "versionSetKey";
  public static final String VERSION_SET_PROPERTIES_ASPECT_NAME = "versionSetProperties";

  // Versioning related
  public static final String INITIAL_VERSION_SORT_ID = "AAAAAAAA";
  public static final String VERSION_SORT_ID_FIELD_NAME = "versionSortId";
  public static final String IS_LATEST_FIELD_NAME = "isLatest";

  public static final String DISPLAY_PROPERTIES_ASPECT_NAME = "displayProperties";

  // Config
  public static final String ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH = "opensearch";
  public static final String ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH = "elasticsearch";

  // DAO
  public static final long LATEST_VERSION = 0;

  // Logging MDC
  public static final String MDC_ENTITY_URN = "entityUrn";
  public static final String MDC_ASPECT_NAME = "aspectName";
  public static final String MDC_ENTITY_TYPE = "entityType";
  public static final String MDC_CHANGE_TYPE = "changeType";

  public static final String RESTLI_SUCCESS = "success";

  private Constants() {}
}
