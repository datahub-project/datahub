package com.linkedin.metadata.search.elasticsearch.query.template;

/**
 * Whether the Search V3 Java fallback can be deleted. This is readiness, not a claim that a
 * production compatibility window has elapsed. The fallback is the V2.5 lexical query, kept under
 * {@code V3SearchQueryBuilder}.
 */
public final class SearchTemplateRetirement {
  /**
   * Shipped {@code elasticsearch.search.v3.templates.enabled} stays false. Parity coverage checks
   * shared clause signals and records intentional DSL differences. Container tests execute shipped
   * templates on Elasticsearch and OpenSearch. That is not a completed canary, so templates are not
   * the configured default.
   */
  public static final boolean TEMPLATES_CONFIGURED_DEFAULT = false;

  /**
   * {@code V3SearchQueryBuilder} still builds the Search V3 Java fallback. Delete its lexical
   * construction only after an operational rollout: shadow, then canary, then a window in which
   * {@code SEARCH_V3_TEMPLATES_ENABLED=false} is no longer required.
   */
  public static final boolean JAVA_LEXICAL_BUILDER_RETIRED = false;

  public static final String DELETION_DEFERRED =
      "V3SearchQueryBuilder lexical construction is retained as the Search V3 Java fallback."
          + " Deletion waits on an operational rollout. Parity tests still record intentional DSL"
          + " differences (exact, structured, and browse), and no production compatibility window"
          + " has elapsed. SEARCH_V3_TEMPLATES_ENABLED=false keeps that Java fallback.";

  private SearchTemplateRetirement() {}
}
