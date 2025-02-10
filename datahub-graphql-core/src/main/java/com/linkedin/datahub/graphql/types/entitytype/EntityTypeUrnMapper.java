package com.linkedin.datahub.graphql.types.entitytype;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.Constants;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * In this class we statically map "well-supported" entity types into a more usable Enum class
 * served by our GraphQL API.
 *
 * <p>When we add new entity types / entity urns, we MAY NEED to update this.
 *
 * <p>Note that we currently do not support mapping entities that fall outside of this set. If you
 * try to map an entity type without a corresponding enum symbol, the mapping WILL FAIL.
 */
public class EntityTypeUrnMapper {

  static final Map<String, String> ENTITY_NAME_TO_ENTITY_TYPE_URN =
      ImmutableMap.<String, String>builder()
          .put(Constants.DOMAIN_ENTITY_NAME, "urn:li:entityType:datahub.domain")
          .put(Constants.DATASET_ENTITY_NAME, "urn:li:entityType:datahub.dataset")
          .put(Constants.CORP_USER_ENTITY_NAME, "urn:li:entityType:datahub.corpuser")
          .put(Constants.CORP_GROUP_ENTITY_NAME, "urn:li:entityType:datahub.corpGroup")
          .put(Constants.DATA_PLATFORM_ENTITY_NAME, "urn:li:entityType:datahub.dataPlatform")
          .put(
              Constants.ER_MODEL_RELATIONSHIP_ENTITY_NAME,
              "urn:li:entityType:datahub.erModelRelationship")
          .put(Constants.DASHBOARD_ENTITY_NAME, "urn:li:entityType:datahub.dashboard")
          .put(Constants.NOTEBOOK_ENTITY_NAME, "urn:li:entityType:datahub.notebook")
          .put(Constants.CHART_ENTITY_NAME, "urn:li:entityType:datahub.chart")
          .put(Constants.DATA_FLOW_ENTITY_NAME, "urn:li:entityType:datahub.dataFlow")
          .put(Constants.DATA_JOB_ENTITY_NAME, "urn:li:entityType:datahub.dataJob")
          .put(Constants.TAG_ENTITY_NAME, "urn:li:entityType:datahub.tag")
          .put(Constants.GLOSSARY_TERM_ENTITY_NAME, "urn:li:entityType:datahub.glossaryTerm")
          .put(Constants.GLOSSARY_NODE_ENTITY_NAME, "urn:li:entityType:datahub.glossaryNode")
          .put(Constants.CONTAINER_ENTITY_NAME, "urn:li:entityType:datahub.container")
          .put(Constants.ML_MODEL_ENTITY_NAME, "urn:li:entityType:datahub.mlModel")
          .put(Constants.ML_MODEL_GROUP_ENTITY_NAME, "urn:li:entityType:datahub.mlModelGroup")
          .put(Constants.ML_FEATURE_TABLE_ENTITY_NAME, "urn:li:entityType:datahub.mlFeatureTable")
          .put(Constants.ML_FEATURE_ENTITY_NAME, "urn:li:entityType:datahub.mlFeature")
          .put(Constants.ML_PRIMARY_KEY_ENTITY_NAME, "urn:li:entityType:datahub.mlPrimaryKey")
          .put(
              Constants.INGESTION_SOURCE_ENTITY_NAME,
              "urn:li:entityType:datahub.dataHubIngestionSource")
          .put(
              Constants.EXECUTION_REQUEST_ENTITY_NAME,
              "urn:li:entityType:datahub.dataHubExecutionRequest")
          .put(Constants.ASSERTION_ENTITY_NAME, "urn:li:entityType:datahub.assertion")
          .put(
              Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME,
              "urn:li:entityType:datahub.dataProcessInstance")
          .put(
              Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME,
              "urn:li:entityType:datahub.dataPlatformInstance")
          .put(Constants.ACCESS_TOKEN_ENTITY_NAME, "urn:li:entityType:datahub.dataHubAccessToken")
          .put(Constants.TEST_ENTITY_NAME, "urn:li:entityType:datahub.test")
          .put(Constants.POLICY_ENTITY_NAME, "urn:li:entityType:datahub.dataHubPolicy")
          .put(Constants.DATAHUB_ROLE_ENTITY_NAME, "urn:li:entityType:datahub.dataHubRole")
          .put(Constants.POST_ENTITY_NAME, "urn:li:entityType:datahub.post")
          .put(Constants.SCHEMA_FIELD_ENTITY_NAME, "urn:li:entityType:datahub.schemaField")
          .put(Constants.DATAHUB_VIEW_ENTITY_NAME, "urn:li:entityType:datahub.dataHubView")
          .put(Constants.QUERY_ENTITY_NAME, "urn:li:entityType:datahub.query")
          .put(Constants.DATA_PRODUCT_ENTITY_NAME, "urn:li:entityType:datahub.dataProduct")
          .put(Constants.OWNERSHIP_TYPE_ENTITY_NAME, "urn:li:entityType:datahub.ownershipType")
          .put(Constants.INCIDENT_ENTITY_NAME, "urn:li:entityType:datahub.incident")
          .put(Constants.ROLE_ENTITY_NAME, "urn:li:entityType:datahub.role")
          .put(
              Constants.STRUCTURED_PROPERTY_ENTITY_NAME,
              "urn:li:entityType:datahub.structuredProperty")
          .put(Constants.FORM_ENTITY_NAME, "urn:li:entityType:datahub.form")
          .put(Constants.DATA_TYPE_ENTITY_NAME, "urn:li:entityType:datahub.dataType")
          .put(Constants.ENTITY_TYPE_ENTITY_NAME, "urn:li:entityType:datahub.entityType")
          .put(Constants.RESTRICTED_ENTITY_NAME, "urn:li:entityType:datahub.restricted")
          .put(
              Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME,
              "urn:li:entityType:datahub.businessAttribute")
          .build();

  private static final Map<String, String> ENTITY_TYPE_URN_TO_NAME =
      ENTITY_NAME_TO_ENTITY_TYPE_URN.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

  private EntityTypeUrnMapper() {}

  public static String getName(String entityTypeUrn) {
    if (!ENTITY_TYPE_URN_TO_NAME.containsKey(entityTypeUrn)) {
      throw new IllegalArgumentException("Unknown entityTypeUrn: " + entityTypeUrn);
    }
    return ENTITY_TYPE_URN_TO_NAME.get(entityTypeUrn);
  }

  /*
   * Takes in a entityTypeUrn and returns a GraphQL EntityType by first mapping
   * the urn to the entity name, and then mapping the entity name to EntityType.
   */
  public static EntityType getEntityType(String entityTypeUrn) {
    if (!ENTITY_TYPE_URN_TO_NAME.containsKey(entityTypeUrn)) {
      throw new IllegalArgumentException("Unknown entityTypeUrn: " + entityTypeUrn);
    }
    final String entityName = ENTITY_TYPE_URN_TO_NAME.get(entityTypeUrn);
    return EntityTypeMapper.getType(entityName);
  }

  @Nonnull
  public static String getEntityTypeUrn(String name) {
    if (!ENTITY_NAME_TO_ENTITY_TYPE_URN.containsKey(name)) {
      throw new IllegalArgumentException("Unknown entity name: " + name);
    }
    return ENTITY_NAME_TO_ENTITY_TYPE_URN.get(name);
  }

  public static boolean isValidEntityType(String entityTypeUrn) {
    return ENTITY_TYPE_URN_TO_NAME.containsKey(entityTypeUrn);
  }
}
