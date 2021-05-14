package com.linkedin.metadata.search.query.builder;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.AutoCompleteResult;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;


@RequiredArgsConstructor
public class AutocompleteQueryBuilder {
  private final EntitySpec _entitySpec;

  private static final Map<String, AutocompleteQueryBuilder> AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME =
      new HashMap<>();

  public static AutocompleteQueryBuilder getBuilder(@Nonnull String entityName) {
    EntitySpec entitySpec = SnapshotEntityRegistry.getInstance().getEntitySpec(entityName);
    return AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME.computeIfAbsent(entityName,
        k -> new AutocompleteQueryBuilder(entitySpec));
  }

  public QueryBuilder getQuery(@Nonnull String query, @Nullable String field) {
    return null;
  }

  public AutoCompleteResult extractResult(@Nonnull SearchResponse searchResponse, @Nonnull String input,
      @Nullable String field, int limit) {
    return null;
  }
}
