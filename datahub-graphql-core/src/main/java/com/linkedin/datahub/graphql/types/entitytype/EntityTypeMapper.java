package com.linkedin.datahub.graphql.types.entitytype;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.Constants;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * This class is for mapping between friendly GraphQL EntityType Enum to the Metadata Service
 * Storage Entities
 */
public class EntityTypeMapper {

  static final Map<EntityType, String> ENTITY_TYPE_TO_NAME =
      ImmutableMap.<EntityType, String>builder()
          .put(EntityType.DOMAIN, Constants.DOMAIN_ENTITY_NAME)
          .put(EntityType.DATASET, Constants.DATASET_ENTITY_NAME)
          .put(EntityType.CORP_USER, Constants.CORP_USER_ENTITY_NAME)
          .put(EntityType.CORP_GROUP, Constants.CORP_GROUP_ENTITY_NAME)
          .put(EntityType.DATA_PLATFORM, Constants.DATA_PLATFORM_ENTITY_NAME)
          .put(EntityType.ER_MODEL_RELATIONSHIP, Constants.ER_MODEL_RELATIONSHIP_ENTITY_NAME)
          .put(EntityType.DASHBOARD, Constants.DASHBOARD_ENTITY_NAME)
          .put(EntityType.NOTEBOOK, Constants.NOTEBOOK_ENTITY_NAME)
          .put(EntityType.CHART, Constants.CHART_ENTITY_NAME)
          .put(EntityType.DATA_FLOW, Constants.DATA_FLOW_ENTITY_NAME)
          .put(EntityType.DATA_JOB, Constants.DATA_JOB_ENTITY_NAME)
          .put(EntityType.TAG, Constants.TAG_ENTITY_NAME)
          .put(EntityType.GLOSSARY_TERM, Constants.GLOSSARY_TERM_ENTITY_NAME)
          .put(EntityType.GLOSSARY_NODE, Constants.GLOSSARY_NODE_ENTITY_NAME)
          .put(EntityType.CONTAINER, Constants.CONTAINER_ENTITY_NAME)
          .put(EntityType.MLMODEL, Constants.ML_MODEL_ENTITY_NAME)
          .put(EntityType.MLMODEL_GROUP, Constants.ML_MODEL_GROUP_ENTITY_NAME)
          .put(EntityType.MLFEATURE_TABLE, Constants.ML_FEATURE_TABLE_ENTITY_NAME)
          .put(EntityType.MLFEATURE, Constants.ML_FEATURE_ENTITY_NAME)
          .put(EntityType.MLPRIMARY_KEY, Constants.ML_PRIMARY_KEY_ENTITY_NAME)
          .put(EntityType.INGESTION_SOURCE, Constants.INGESTION_SOURCE_ENTITY_NAME)
          .put(EntityType.EXECUTION_REQUEST, Constants.EXECUTION_REQUEST_ENTITY_NAME)
          .put(EntityType.ASSERTION, Constants.ASSERTION_ENTITY_NAME)
          .put(EntityType.DATA_PROCESS_INSTANCE, Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME)
          .put(EntityType.DATA_PLATFORM_INSTANCE, Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME)
          .put(EntityType.ACCESS_TOKEN, Constants.ACCESS_TOKEN_ENTITY_NAME)
          .put(EntityType.TEST, Constants.TEST_ENTITY_NAME)
          .put(EntityType.DATAHUB_POLICY, Constants.POLICY_ENTITY_NAME)
          .put(EntityType.DATAHUB_ROLE, Constants.DATAHUB_ROLE_ENTITY_NAME)
          .put(EntityType.POST, Constants.POST_ENTITY_NAME)
          .put(EntityType.SCHEMA_FIELD, Constants.SCHEMA_FIELD_ENTITY_NAME)
          .put(EntityType.DATAHUB_VIEW, Constants.DATAHUB_VIEW_ENTITY_NAME)
          .put(EntityType.QUERY, Constants.QUERY_ENTITY_NAME)
          .put(EntityType.DATA_PRODUCT, Constants.DATA_PRODUCT_ENTITY_NAME)
          .put(EntityType.CUSTOM_OWNERSHIP_TYPE, Constants.OWNERSHIP_TYPE_ENTITY_NAME)
          .put(EntityType.INCIDENT, Constants.INCIDENT_ENTITY_NAME)
          .put(EntityType.ROLE, Constants.ROLE_ENTITY_NAME)
          .put(EntityType.STRUCTURED_PROPERTY, Constants.STRUCTURED_PROPERTY_ENTITY_NAME)
          .put(EntityType.FORM, Constants.FORM_ENTITY_NAME)
          .put(EntityType.DATA_TYPE, Constants.DATA_TYPE_ENTITY_NAME)
          .put(EntityType.ENTITY_TYPE, Constants.ENTITY_TYPE_ENTITY_NAME)
          .put(EntityType.RESTRICTED, Constants.RESTRICTED_ENTITY_NAME)
          .put(EntityType.BUSINESS_ATTRIBUTE, Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)
          .build();

  private static final Map<String, EntityType> ENTITY_NAME_TO_TYPE =
      ENTITY_TYPE_TO_NAME.entrySet().stream()
          .collect(Collectors.toMap(e -> e.getValue().toLowerCase(), Map.Entry::getKey));

  private EntityTypeMapper() {}

  public static EntityType getType(String name) {
    String lowercaseName = name.toLowerCase();
    if (!ENTITY_NAME_TO_TYPE.containsKey(lowercaseName)) {
      return EntityType.OTHER;
    }
    return ENTITY_NAME_TO_TYPE.get(lowercaseName);
  }

  @Nonnull
  public static String getName(EntityType type) {
    if (!ENTITY_TYPE_TO_NAME.containsKey(type)) {
      throw new IllegalArgumentException("Unknown entity type: " + type);
    }
    return ENTITY_TYPE_TO_NAME.get(type);
  }
}
