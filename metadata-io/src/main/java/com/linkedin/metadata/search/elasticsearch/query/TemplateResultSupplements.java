package com.linkedin.metadata.search.elasticsearch.query;

import javax.annotation.Nonnull;

/** The registered {@link TemplateResultSupplement}. OSS leaves the no-op in place. */
public final class TemplateResultSupplements {
  private static volatile TemplateResultSupplement current = new TemplateResultSupplement.None();

  private TemplateResultSupplements() {}

  public static void register(@Nonnull TemplateResultSupplement supplement) {
    current = supplement;
  }

  @Nonnull
  static TemplateResultSupplement get() {
    return current;
  }
}
