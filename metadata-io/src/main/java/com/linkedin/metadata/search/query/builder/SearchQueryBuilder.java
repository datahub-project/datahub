package com.linkedin.metadata.search.query.builder;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.index.query.QueryBuilder;


@RequiredArgsConstructor
public class SearchQueryBuilder {
  private final EntitySpec _entitySpec;

  private static final Map<String, SearchQueryBuilder> QUERY_BUILDER_BY_ENTITY_NAME = new HashMap<>();

  public static SearchQueryBuilder getBuilder(@Nonnull String entityName) {
    EntitySpec entitySpec = SnapshotEntityRegistry.getInstance().getEntitySpec(entityName);
    return QUERY_BUILDER_BY_ENTITY_NAME.computeIfAbsent(entityName, k -> new SearchQueryBuilder(entitySpec));
  }

  public QueryBuilder getQuery(@Nonnull String query) {
    return null;
  }
}
