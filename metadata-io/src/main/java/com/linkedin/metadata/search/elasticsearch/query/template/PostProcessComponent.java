package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.SearchEntity;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Reorders the fetched keyword window. Stage 2 rescores the main window. Stage 3 runs after the
 * supplementary merge and reads that merged entity list.
 *
 * <p>The ranking-stage methods tell the shared query path which fields to fetch and how to read the
 * request flags. The OSS implementation returns empty fields and does not rewrite the query.
 */
public interface PostProcessComponent extends SearchTemplateComponent {
  @Nonnull
  List<SearchEntity> rescore(
      @Nonnull SearchTemplateComponentRequest request, @Nonnull List<SearchEntity> entities);

  @Nonnull
  List<SearchEntity> postProcess(
      @Nonnull SearchTemplateComponentRequest request, @Nonnull List<SearchEntity> mergedEntities);

  @Nonnull
  Set<String> sourceFields(@Nonnull MergedSearchTemplate template);

  @Nonnull
  List<String> matchTierSourceFields(@Nonnull Map<String, String> config);

  boolean runsMatchTier(@Nonnull Map<String, String> config);

  @Nonnull
  List<String> matchTierFieldSources(@Nonnull Map<String, String> config);

  boolean includeExplain(@Nullable SearchFlags flags);

  boolean rescoreEnabled(@Nullable SearchFlags flags);

  /** How many hits to fetch before this stage reorders them. */
  int fetchSize(int from, @Nullable Integer size, int window);

  @Nonnull
  String normalizeQuery(@Nonnull String query);
}
