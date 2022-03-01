package com.linkedin.datahub.graphql.resolvers.search;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.EntityType;
import java.util.List;


public class SearchUtils {
  private SearchUtils() {
  }

  public static final List<EntityType> SEARCHABLE_ENTITY_TYPES =
      ImmutableList.of(EntityType.DATASET, EntityType.DASHBOARD, EntityType.CHART, EntityType.MLMODEL,
          EntityType.MLMODEL_GROUP, EntityType.MLFEATURE_TABLE, EntityType.DATA_FLOW, EntityType.DATA_JOB,
          EntityType.GLOSSARY_TERM, EntityType.TAG, EntityType.CORP_USER, EntityType.CORP_GROUP, EntityType.CONTAINER,
          EntityType.DOMAIN);
}
