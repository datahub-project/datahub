package io.datahubproject.openapi.util;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
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
      if (searchTemplate.isBlank()) {
        throw new IllegalArgumentException("searchTemplate must be a searchTemplate URN");
      }
      flags.setSearchTemplate(requireSearchTemplate(searchTemplate));
    }
    if (includeExplain != null) {
      flags.setIncludeExplain(includeExplain);
    }
    return flags;
  }

  @Nonnull
  private static Urn requireSearchTemplate(@Nonnull String value) {
    Urn urn = UrnUtils.requireUrn(value);
    if (!SEARCH_TEMPLATE_ENTITY_NAME.equals(urn.getEntityType())) {
      throw new IllegalArgumentException("searchTemplate must be a searchTemplate URN: " + value);
    }
    return urn;
  }
}
