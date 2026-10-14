package com.linkedin.metadata.search.elasticsearch.query.template;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Named splice points in a query body. Where a tag sits inside the JSON is not checked. */
public final class SearchTemplateHoles {
  private static final Pattern TAG = Pattern.compile("\\{\\{[#&/]?\\s*([A-Za-z0-9_]+)\\s*}}");
  private static final Pattern SPLICE = Pattern.compile("\\{\\{&\\s*([A-Za-z0-9_]+)\\s*}}");

  private SearchTemplateHoles() {}

  public static boolean contains(@Nullable String query, @Nonnull String name) {
    if (query == null || name.isEmpty()) {
      return false;
    }
    Matcher matcher = TAG.matcher(query);
    while (matcher.find()) {
      if (name.equals(matcher.group(1))) {
        return true;
      }
    }
    return false;
  }

  /** Names inserted with an unescaped tag, such as {@code {{&extraDisMaxQueries}}}. */
  @Nonnull
  public static Set<String> splices(@Nullable String source) {
    Set<String> names = new LinkedHashSet<>();
    if (source == null) {
      return names;
    }
    Matcher matcher = SPLICE.matcher(source);
    while (matcher.find()) {
      names.add(matcher.group(1));
    }
    return names;
  }

  public static void requirePresent(@Nullable String query, @Nonnull Collection<String> names) {
    for (String name : names) {
      if (!contains(query, name)) {
        throw new SearchTemplateException("Search template query hole is missing: " + name);
      }
    }
  }
}
