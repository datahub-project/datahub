package com.linkedin.metadata.search.elasticsearch.query.template;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * Equivalence lines from the synonym files the analyzers already load. Explicit {@code a => x, y}
 * rules are skipped: as whole-term synonyms they scored {@code cac} as an exact name match for
 * {@code customer}.
 */
public final class SearchTemplateSynonyms {
  private static volatile Map<String, List<String>> installed;

  private SearchTemplateSynonyms() {}

  /** The query builder installs the catalog when it is constructed. */
  public static void installFromClasspath() {
    installed = load();
  }

  @Nonnull
  private static Map<String, List<String>> current() {
    Map<String, List<String>> catalog = installed;
    if (catalog != null) {
      return catalog;
    }
    synchronized (SearchTemplateSynonyms.class) {
      if (installed == null) {
        installed = load();
      }
      return installed;
    }
  }

  /** Other members of the equivalence class for the query and for each of its tokens. */
  @Nonnull
  public static List<String> expansions(@Nonnull String query) {
    String text = query.toLowerCase(Locale.ROOT).trim();
    if (text.isEmpty()) {
      return List.of();
    }
    Set<String> found = new LinkedHashSet<>();
    addOthers(found, text);
    for (String token : text.split("\\s+")) {
      addOthers(found, token);
    }
    found.remove(text);
    return List.copyOf(found);
  }

  private static void addOthers(Set<String> found, String term) {
    List<String> others = current().get(term);
    if (others != null) {
      found.addAll(others);
    }
  }

  @Nonnull
  private static Map<String, List<String>> load() {
    Map<String, List<String>> map = new LinkedHashMap<>();
    try {
      Resource[] resources =
          new PathMatchingResourcePatternResolver()
              .getResources("classpath*:elasticsearch/synonyms/*.txt");
      for (Resource resource : resources) {
        try (InputStream in = resource.getInputStream();
            BufferedReader reader =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            String trimmed = line.trim().toLowerCase(Locale.ROOT);
            if (trimmed.isEmpty() || trimmed.startsWith("#") || trimmed.contains("=>")) {
              continue;
            }
            List<String> terms = new ArrayList<>();
            for (String part : trimmed.split(",")) {
              String term = part.trim();
              if (!term.isEmpty()) {
                terms.add(term);
              }
            }
            if (terms.size() < 2) {
              continue;
            }
            for (String term : terms) {
              List<String> others = new ArrayList<>();
              for (String other : terms) {
                if (!other.equals(term)) {
                  others.add(other);
                }
              }
              map.put(term, others);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new SearchTemplateException("Failed to read search synonym files", e);
    }
    return Map.copyOf(map);
  }
}
