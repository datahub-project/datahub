package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Dialect is the body language. Engine version stays a selection key inside a dialect. */
public final class SearchTemplateDialects {
  public static final String ELASTICSEARCH = "elasticsearch";
  public static final String POSTGRES = "postgres";

  private SearchTemplateDialects() {}

  @Nonnull
  public static String requestDialect(@Nullable SearchClientShim.SearchEngineType engineType) {
    return ELASTICSEARCH;
  }

  /**
   * Family plus major version, no separator. Unknown engines use the shared {@code default} slot.
   */
  @Nonnull
  public static String engineKey(@Nullable SearchClientShim.SearchEngineType engineType) {
    if (engineType == null || engineType == SearchClientShim.SearchEngineType.UNKNOWN) {
      return "default";
    }
    if (!engineType.isElasticsearch() && !engineType.isOpenSearch()) {
      return "default";
    }
    return engineType.getEngine() + engineType.getMajorVersion();
  }

  public static boolean matches(@Nullable String templateDialect, @Nonnull String requestDialect) {
    if (templateDialect == null || templateDialect.isBlank()) {
      return ELASTICSEARCH.equals(requestDialect);
    }
    return templateDialect.equalsIgnoreCase(requestDialect);
  }
}
