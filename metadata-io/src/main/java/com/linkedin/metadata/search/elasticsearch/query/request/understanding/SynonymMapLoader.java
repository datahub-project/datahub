package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Parses an Elasticsearch synonym file into a term → synonyms map, so query building can expand
 * terms that never pass through a synonym analyzer (term queries on keyword fields, phrase
 * prefixes).
 */
@Slf4j
public final class SynonymMapLoader {

  /** The synonym rules the default search analyzers apply, so both sides expand alike. */
  public static final String DEFAULT_SYNONYMS_RESOURCE = "elasticsearch/synonyms/default.txt";

  private SynonymMapLoader() {}

  /**
   * Loads {@code resource} from the classpath. Equivalence lines ({@code a, b, c}) map every term
   * to the whole group; explicit mappings ({@code a, b => x, y}) map only the left-hand terms to
   * the right-hand ones, as Elasticsearch applies them. Returns an empty map when the file is
   * missing or unreadable, which only turns synonym expansion off.
   */
  @Nonnull
  public static Map<String, Set<String>> loadFromClasspath(@Nonnull final String resource) {
    Map<String, Set<String>> synonymMap = new HashMap<>();
    try {
      InputStream is = SynonymMapLoader.class.getClassLoader().getResourceAsStream(resource);
      if (is == null) {
        log.info("Synonym file {} not found on classpath; synonym expansion disabled", resource);
        return synonymMap;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          line = line.trim();
          if (line.isEmpty() || line.startsWith("#")) {
            continue;
          }
          if (line.contains("=>")) {
            String[] parts = line.split("=>", 2);
            Set<String> lhsTerms = parseTerms(parts[0]);
            Set<String> rhsTerms = parseTerms(parts[1]);
            for (String term : lhsTerms) {
              synonymMap.computeIfAbsent(term, k -> new HashSet<>()).addAll(rhsTerms);
            }
          } else {
            Set<String> allTerms = parseTerms(line);
            for (String term : allTerms) {
              synonymMap.computeIfAbsent(term, k -> new HashSet<>()).addAll(allTerms);
            }
          }
        }
      }
    } catch (Exception e) {
      log.warn("Failed to load synonym file {}; synonym expansion disabled", resource, e);
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
