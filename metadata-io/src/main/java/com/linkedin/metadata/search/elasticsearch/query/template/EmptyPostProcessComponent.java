package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.search.template.SearchTemplateComponentMode;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Registered {@code postProcess} v1 when no other implementation occupies that name and version. It
 * accepts the stored config and does not reorder hits.
 */
public final class EmptyPostProcessComponent implements SearchTemplateComponent {
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
      @Nonnull SearchTemplateComponentRequest request, @Nonnull Map<String, String> config) {
    return request.getFrom();
  }

  @Override
  @Nonnull
  public SearchTemplateComponentPage execute(@Nonnull SearchTemplateComponentRequest request) {
    return new SearchTemplateComponentPage(
        request.getHits(),
        request.getTotal(),
        request.getMetadata(),
        List.of(),
        false,
        false,
        null);
  }
}
