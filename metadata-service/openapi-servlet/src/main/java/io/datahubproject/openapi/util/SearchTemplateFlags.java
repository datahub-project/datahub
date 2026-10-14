package io.datahubproject.openapi.util;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.query.SearchFlags;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Caller-selected template and explain flag shared by the entity scroll APIs. */
public final class SearchTemplateFlags {
  private SearchTemplateFlags() {}

  @Nonnull
  public static SearchFlags apply(
      @Nonnull SearchFlags flags,
      @Nullable String searchTemplate,
      @Nullable Boolean includeExplain) {
    if (searchTemplate != null) {
      flags.setSearchTemplate(UrnUtils.getUrn(searchTemplate));
    }
    if (includeExplain != null) {
      flags.setIncludeExplain(includeExplain);
    }
    return flags;
  }
}
