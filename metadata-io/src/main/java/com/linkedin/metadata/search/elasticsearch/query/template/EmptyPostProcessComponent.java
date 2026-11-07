package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.search.template.SearchTemplateComponentMode;
import com.linkedin.search.template.SearchTemplateRanking;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Registered {@code postProcess} v1 when no other implementation occupies that name and version. It
 * accepts the stored config and does not reorder hits.
 */
public final class EmptyPostProcessComponent implements PostProcessComponent {
  public static final String NAME = "postProcess";
  public static final int VERSION = 1;

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  public int version() {
    return VERSION;
  }

  @Override
  public void validate(
      @Nonnull SearchTemplateComponentMode mode, @Nonnull Map<String, String> config) {
    if (mode != SearchTemplateComponentMode.POST_PROCESS) {
      throw new SearchTemplateException("postProcess is POST_PROCESS only");
    }
  }

  /**
   * Returns the caller's offset so the fetch stays the requested page. A larger window would pull
   * hits this implementation does not reorder.
   */
  @Override
  public int plan(
      @Nonnull SearchTemplateComponentRequest request,
      @Nonnull Map<String, String> config,
      @Nullable SearchTemplateRanking ranking) {
    return request.getFrom();
  }

  @Override
  @Nonnull
  public List<SearchEntity> rescore(
      @Nonnull SearchTemplateComponentRequest request, @Nonnull List<SearchEntity> entities) {
    return entities;
  }

  @Override
  @Nonnull
  public List<SearchEntity> postProcess(
      @Nonnull SearchTemplateComponentRequest request, @Nonnull List<SearchEntity> mergedEntities) {
    return mergedEntities;
  }

  @Override
  @Nonnull
  public Set<String> sourceFields(@Nonnull MergedSearchTemplate template) {
    return Set.of();
  }

  @Override
  @Nonnull
  public List<String> matchTierSourceFields(@Nonnull Map<String, String> config) {
    return List.of();
  }

  @Override
  public boolean runsMatchTier(@Nonnull Map<String, String> config) {
    return false;
  }

  @Override
  @Nonnull
  public List<String> matchTierFieldSources(@Nonnull Map<String, String> config) {
    return List.of();
  }

  @Override
  public boolean includeExplain(@Nullable SearchFlags flags) {
    return flags != null && flags.hasIncludeExplain() && flags.isIncludeExplain();
  }

  @Override
  public boolean rescoreEnabled(@Nullable SearchFlags flags) {
    return true;
  }

  @Override
  public int fetchSize(int from, @Nullable Integer size, int window) {
    int page = size == null ? 10 : size;
    long requestedEnd = (long) from + page;
    return (int) Math.min(Math.max(window, requestedEnd), 10_000);
  }

  @Override
  @Nonnull
  public String normalizeQuery(@Nonnull String query) {
    return query;
  }
}
