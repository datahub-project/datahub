package com.linkedin.metadata.search.query.builder;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.MatchMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;


@RequiredArgsConstructor
public class SearchHighlightsBuilder {
  private final EntitySpec _entitySpec;

  private static final Map<String, SearchHighlightsBuilder> HIGHLIGHTS_BUILDER_BY_ENTITY_NAME = new HashMap<>();

  public static SearchHighlightsBuilder getBuilder(@Nonnull String entityName) {
    EntitySpec entitySpec = SnapshotEntityRegistry.getInstance().getEntitySpec(entityName);
    return HIGHLIGHTS_BUILDER_BY_ENTITY_NAME.computeIfAbsent(entityName, k -> new SearchHighlightsBuilder(entitySpec));
  }

  public HighlightBuilder getHighlights() {
    return null;
  }

  public List<MatchMetadata> extractHighlights(@Nonnull SearchResponse searchResponse) {
    return null;
  }
}
