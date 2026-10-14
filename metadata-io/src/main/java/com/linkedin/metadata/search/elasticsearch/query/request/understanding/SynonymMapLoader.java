package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * Parses an Elasticsearch synonym file into a term → synonyms map, so query building can expand
 * terms that never pass through a synonym analyzer (term queries on keyword fields, phrase
 * prefixes).
 */
@Slf4j
public final class SynonymMapLoader {

  /** The synonym files the search analyzers load, resolved the same way. */
  private static final String SYNONYM_FILES = "classpath:elasticsearch/synonyms/*.txt";

  private SynonymMapLoader() {}

  /**
   * Loads and merges every synonym file the search analyzers apply. Equivalence lines ({@code a, b,
   * c}) map every term to the whole group. Explicit mappings ({@code a, b => x, y}) are skipped:
   * they split phrases into tokens for the analyzers and are not whole-term synonyms. Returns an
   * empty map when no file loads, which only turns synonym expansion off.
   */
  @Nonnull
  public static Map<String, Set<String>> loadDefault() {
    Map<String, Set<String>> synonymMap = new HashMap<>();
    try {
      Resource[] resources = new PathMatchingResourcePatternResolver().getResources(SYNONYM_FILES);
      if (resources.length == 0) {
        log.warn("No synonym files match {}; synonym expansion disabled", SYNONYM_FILES);
      }
      for (Resource resource : resources) {
        try (InputStream is = resource.getInputStream()) {
          read(is)
              .forEach(
                  (term, synonyms) ->
                      synonymMap.computeIfAbsent(term, k -> new HashSet<>()).addAll(synonyms));
        }
      }
    } catch (Exception e) {
      log.warn("Failed to load synonym files {}; synonym expansion disabled", SYNONYM_FILES, e);
    }
    return synonymMap;
  }

  private static Map<String, Set<String>> read(@Nonnull InputStream is) throws IOException {
    Map<String, Set<String>> synonymMap = new HashMap<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        if (line.contains("=>")) {
          // Explicit mappings expand a phrase into tokens for the analyzers ("big query =>
          // bigquery, big, query"); as whole-term synonyms they would match unrelated names
          continue;
        }
        Set<String> allTerms = parseTerms(line);
        for (String term : allTerms) {
          synonymMap.computeIfAbsent(term, k -> new HashSet<>()).addAll(allTerms);
        }
      }
    }
    return synonymMap;
  }

  private static Set<String> parseTerms(@Nonnull final String csv) {
    Set<String> terms = new HashSet<>();
    for (String token : csv.split(",")) {
      String t = token.trim().toLowerCase();
      if (!t.isEmpty()) {
        terms.add(t);
      }
    }
    return terms;
  }
}
