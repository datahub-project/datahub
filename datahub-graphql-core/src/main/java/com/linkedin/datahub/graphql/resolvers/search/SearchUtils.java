package com.linkedin.datahub.graphql.resolvers.search;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.EntityType;
import java.util.List;


public class SearchUtils {
  private SearchUtils() {
  }

  /**
   * Entities that are searched by default in Search Across Entities
   */
  public static final List<EntityType> SEARCHABLE_ENTITY_TYPES =
      ImmutableList.of(
          EntityType.DATASET,
          EntityType.DASHBOARD,
          EntityType.CHART,
          EntityType.MLMODEL,
          EntityType.MLMODEL_GROUP,
          EntityType.MLFEATURE_TABLE,
          EntityType.MLFEATURE,
          EntityType.MLPRIMARY_KEY,
          EntityType.DATA_FLOW,
          EntityType.DATA_JOB,
          EntityType.GLOSSARY_TERM,
          EntityType.TAG,
          EntityType.CORP_USER,
          EntityType.CORP_GROUP,
          EntityType.CONTAINER,
          EntityType.DOMAIN,
          EntityType.NOTEBOOK);

  /**
   * Entities that are part of autocomplete by default in Auto Complete Across Entities
   */
  public static final List<EntityType> AUTO_COMPLETE_ENTITY_TYPES =
      ImmutableList.of(
          EntityType.DATASET,
          EntityType.DASHBOARD,
          EntityType.CHART,
          EntityType.MLMODEL,
          EntityType.MLMODEL_GROUP,
          EntityType.MLFEATURE_TABLE,
          EntityType.DATA_FLOW,
          EntityType.DATA_JOB,
          EntityType.GLOSSARY_TERM,
          EntityType.TAG,
          EntityType.CORP_USER,
          EntityType.CORP_GROUP,
          EntityType.NOTEBOOK);
}
