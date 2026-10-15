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
  private boolean enabled = false;
  private int maxLeafClauses = 1024;

  /** {@code storage} on GMS. System update sets {@code classpath} before its context starts. */
  private SearchTemplateSource source = SearchTemplateSource.STORAGE;

  private SearchTemplateConcurrencyConfiguration concurrency =
      new SearchTemplateConcurrencyConfiguration();

  /** Seed name to slot map. Slot values are template URN strings. */
  private Map<String, Map<String, Object>> defaults = new LinkedHashMap<>();

  /** surface -> intent -> slot map. */
  private Map<String, Map<String, Map<String, Object>>> surfaces = new LinkedHashMap<>();
}
