package com.linkedin.datahub.graphql.resolvers;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.generated.EntityType;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * This class is for mapping between friendly GraphQL EntityType Enum to the Metadata Service Storage Entities
 */
public class EntityTypeMapper {

  static final Map<EntityType, String> ENTITY_TYPE_TO_NAME =
      ImmutableMap.<EntityType, String>builder()
          .put(EntityType.DATASET, "dataset")
          .put(EntityType.CORP_USER, "corpuser")
          .put(EntityType.CORP_GROUP, "corpGroup")
          .put(EntityType.DATA_PLATFORM, "dataPlatform")
          .put(EntityType.DASHBOARD, "dashboard")
          .put(EntityType.CHART, "chart")
          .put(EntityType.TAG, "tag")
          .put(EntityType.DATA_FLOW, "dataFlow")
          .put(EntityType.DATA_JOB, "dataJob")
          .put(EntityType.GLOSSARY_TERM, "glossaryTerm")
          .put(EntityType.MLMODEL, "mlModel")
          .put(EntityType.MLMODEL_GROUP, "mlModelGroup")
          .put(EntityType.MLFEATURE_TABLE, "mlFeatureTable")
          .put(EntityType.MLFEATURE, "mlFeature")
          .put(EntityType.MLPRIMARY_KEY, "mlPrimaryKey")
          .put(EntityType.CONTAINER, "container")
          .put(EntityType.DOMAIN, "domain")
          .put(EntityType.NOTEBOOK, "notebook")
          .put(EntityType.DATA_PLATFORM_INSTANCE, "dataPlatformInstance")
          .build();

  private static final Map<String, EntityType> ENTITY_NAME_TO_TYPE =
      ENTITY_TYPE_TO_NAME.entrySet().stream().collect(Collectors.toMap(e -> e.getValue().toLowerCase(), Map.Entry::getKey));

  private EntityTypeMapper() {
  }

  public static EntityType getType(String name) {
    String lowercaseName = name.toLowerCase();
    if (!ENTITY_NAME_TO_TYPE.containsKey(lowercaseName)) {
      throw new IllegalArgumentException("Unknown entity name: " + name);
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
