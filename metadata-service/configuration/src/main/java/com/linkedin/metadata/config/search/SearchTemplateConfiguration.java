package com.linkedin.metadata.config.search;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Search V3 query template selection. {@code defaults} and each surface map engine slots ({@code
 * default}, {@code elasticsearch8}, {@code opensearch3}, ...) to seed template URNs. {@code
 * fulltext} is the seed when fulltext is on. {@code structured} is the seed when it is off.
 */
@Data
@Accessors(chain = true)
public class SearchTemplateConfiguration {
  /** Bound from application.yaml. Shipped false, which is the rollback to the Java builder. */
  private boolean enabled;

  /**
   * Per-surface off, shadow, and canary controls. Ignored while {@link #enabled} is false, which is
   * the shipped rollback to the Java V2.5 builder.
   */
  private SearchTemplateRolloutConfiguration rollout = new SearchTemplateRolloutConfiguration();

  /**
   * Bound from application.yaml. When templates are enabled, a non-positive value fails startup.
   * While enabled, a render over this cap is rejected.
   */
  private int maxLeafClauses;

  /**
   * Bound from application.yaml. Seconds to wait for a document rerank before keeping keyword
   * order.
   */
  private int documentRerankTimeoutSeconds = 2;

  /** {@code storage} on GMS. System update sets {@code classpath} before its context starts. */
  private SearchTemplateSource source = SearchTemplateSource.STORAGE;

  /** Bound from application.yaml. Zero drops the keyword contribution of a document rerank. */
  private double hybridKeywordScale;

  /** Bound from application.yaml. Must be positive. */
  private int maxParentDepth;

  /** Bound from application.yaml. Must be positive. */
  private int cacheMaximumSize;

  /** Bound from application.yaml. A template may not store more route patterns than this. */
  private int maxRoutes;

  /** Bound from application.yaml. A stored route regex may not be longer than this. */
  private int maxRoutePatternLength;

  /** Bound from application.yaml. Selection may not follow more templates than this. */
  private int maxRouteHops;

  private SearchTemplateConcurrencyConfiguration concurrency =
      new SearchTemplateConcurrencyConfiguration();

  /** Seed name to slot map. Slot values are template URN strings. */
  private Map<String, Map<String, Object>> defaults = new LinkedHashMap<>();

  /** surface -> intent -> slot map. */
  private Map<String, Map<String, Map<String, Object>>> surfaces = new LinkedHashMap<>();
}
