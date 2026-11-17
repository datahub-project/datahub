package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.search.template.SearchTemplateComponentMode;
import com.linkedin.search.template.SearchTemplateRanking;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One registered template component. Implementations are registered by name and contract version.
 */
public interface SearchTemplateComponent {
  @Nonnull
  String name();

  int version();

  /** Rejects a mode or config this implementation cannot run. Called at write and at bind. */
  void validate(@Nonnull SearchTemplateComponentMode mode, @Nonnull Map<String, String> config);

  /**
   * Candidate window to fetch for this request. Called before the engine query. The window can
   * depend on the query and resolved ranking, so it is not fixed at write time.
   */
  int plan(
      @Nonnull SearchTemplateComponentRequest request,
      @Nonnull Map<String, String> config,
      @Nullable SearchTemplateRanking ranking);
}
