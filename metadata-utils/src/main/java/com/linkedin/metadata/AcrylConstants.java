package com.linkedin.metadata;

public class AcrylConstants {

  public static final String ACTION_REQUEST_TYPE_TERM_PROPOSAL = "TERM_ASSOCIATION";
  public static final String ACTION_REQUEST_TYPE_TAG_PROPOSAL = "TAG_ASSOCIATION";
  public static final String ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL =
      "CREATE_GLOSSARY_NODE";
  public static final String ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL =
      "CREATE_GLOSSARY_TERM";
  public static final String ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL = "UPDATE_DESCRIPTION";
  public static final String ACTION_REQUEST_TYPE_DATA_CONTRACT_PROPOSAL = "DATA_CONTRACT";
  public static final String ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL =
      "STRUCTURED_PROPERTY_ASSOCIATION";
  public static final String ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL = "DOMAIN_ASSOCIATION";
  public static final String ACTION_REQUEST_TYPE_OWNER_PROPOSAL = "OWNER_ASSOCIATION";
  public static final String ACTION_REQUEST_STATUS_PENDING = "PENDING";
  public static final String ACTION_REQUEST_STATUS_COMPLETE = "COMPLETED";
  public static final String ACTION_REQUEST_RESULT_ACCEPTED = "ACCEPTED";
  public static final String ACTION_REQUEST_RESULT_REJECTED = "REJECTED";

  // For entity change events
  public static final String ACTION_REQUEST_STATUS_KEY = "actionRequestStatus";
  public static final String ACTION_REQUEST_TYPE_KEY = "actionRequestType";
  public static final String ACTION_REQUEST_RESULT_KEY = "actionRequestResult";
  public static final String RESOURCE_TYPE_KEY = "resourceType";
  public static final String RESOURCE_URN_KEY = "resourceUrn";
  public static final String SUB_RESOURCE_TYPE_KEY = "subResourceType";
  public static final String SUB_RESOURCE_KEY = "subResource";
  public static final String GLOSSARY_TERM_URN_KEY = "glossaryTermUrn";
  public static final String TAG_URN_KEY = "tagUrn";
  public static final String GLOSSARY_ENTITY_NAME_KEY = "glossaryEntityName";
  public static final String PARENT_NODE_URN_KEY = "parentNodeUrn";
  public static final String DESCRIPTION_KEY = "description";

  // For tests
  public static final String PASSING_TESTS_FIELD = "passingTests";
  public static final String FAILING_TESTS_FIELD = "failingTests";

  public static final String PASSING_TESTS_MD5_FIELD = "passingTestDefinitionMd5";
  public static final String FAILING_TESTS_MD5_FIELD = "failingTestDefinitionMd5";
  public static final String TESTS_CREATED_TIME_INDEX_FIELD_NAME = "createdTime";
  public static final String TESTS_LAST_UPDATED_TIME_INDEX_FIELD_NAME = "lastUpdatedTime";
  public static final String BATCH_TEST_RUN_EVENT_ASPECT_NAME = "batchTestRunEvent";

  // For system monitors
  public static final String FRESHNESS_SYSTEM_MONITOR_ID = "__system_freshness";
  public static final String ACRYL_LOGO_FILE_PATH = "/integrations/static/acryl-slack-icon.png";

  // For subscriptions
  public static final String SUBSCRIPTION_ENTITY_NAME = "subscription";
  public static final String SUBSCRIPTION_INFO_ASPECT_NAME = "subscriptionInfo";
  public static final String ENTITY_URN_FIELD_NAME = "entityUrn";
  public static final String ACTOR_URN_FIELD_NAME = "actorUrn";
  public static final String ACTOR_TYPE_FIELD_NAME = "actorType";
  public static final String ENTITY_CHANGE_TYPES_FIELD_NAME = "entityChangeTypes";
  public static final String ENTITY_CHANGE_TYPES_FILTER_INCLUDE_ASSERTIONS_FIELD_NAME =
      "subscribedAssertions";
  public static final String SUBSCRIPTION_TYPES_FIELD_NAME = "subscriptionTypes";
  // For notifications
  public static final String NOTIFICATION_SETTINGS_ASPECT_NAME = "notificationSettings";

  public static final String FORWARDING_ACTION_URN =
      "urn:li:dataHubAction:__system__forwardingAction";
  public static final String METADATA_TESTS_FORWARDING_ACTION_DISPLAY_NAME = "Forwarding Action";
  public static final String METADATA_TESTS_FORWARDING_ACTION_TYPE =
      "datahub_integrations.actions.forward.forwarding_action.ForwardingAction";

  // Personas
  public static final String PERSONA_ENTITY_NAME = "dataHubPersona";
  public static final String PERSONA_INFO_ASPECT_NAME = "dataHubPersonaInfo";

  // Remote executors
  public static final String REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME =
      "dataHubRemoteExecutorGlobalConfig";
  public static final String REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME =
      "dataHubRemoteExecutorPoolGlobalConfig";
  public static final String REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_SINGLETON_URN =
      "urn:li:dataHubRemoteExecutorGlobalConfig:primary";
  public static final String REMOTE_EXECUTOR_POOL_ENTITY_NAME = "dataHubRemoteExecutorPool";
  public static final String REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME =
      "dataHubRemoteExecutorPoolInfo";
  public static final String REMOTE_EXECUTOR_ENTITY_NAME = "dataHubRemoteExecutor";
  public static final String REMOTE_EXECUTOR_STATUS_ASPECT_NAME = "dataHubRemoteExecutorStatus";

  private AcrylConstants() {}
}
