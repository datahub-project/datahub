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
  private static final Pattern SECTION = Pattern.compile("\\{\\{#\\s*([A-Za-z0-9_]+)\\s*}}");

  private SearchTemplateHoles() {}

  /**
   * Rejects an unescaped user string. Triple-stash and a delimiter change skip JSON escaping. An
   * unescaped tag is kept for server-built JSON and for fragment holes.
   */
  public static void rejectUnescaped(
      @Nonnull String section, @Nullable String template, @Nonnull Set<String> fragments) {
    if (template == null || template.isEmpty()) {
      return;
    }
    int cursor = 0;
    while (cursor < template.length()) {
      int open = template.indexOf("{{", cursor);
      if (open < 0) {
        return;
      }
      int sigil = skipSpace(template, open + 2);
      if (sigil >= template.length()) {
        return;
      }
      char kind = template.charAt(sigil);
      if (kind == '{') {
        throw new SearchTemplateException(
            "Search template " + section + " contains an unescaped tag");
      }
      if (kind == '=') {
        throw new SearchTemplateException(
            "Search template " + section + " cannot change delimiters");
      }
      if (kind == '&') {
        int nameStart = skipSpace(template, sigil + 1);
        int nameEnd = nameStart;
        while (nameEnd < template.length()) {
          char c = template.charAt(nameEnd);
          if (c == '}' || c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            break;
          }
          nameEnd++;
        }
        String name = template.substring(nameStart, nameEnd);
        if (!allowedUnescaped(name, fragments, template)) {
          throw new SearchTemplateException("Search template " + section + " unescapes " + name);
        }
      }
      cursor = open + 2;
    }
  }

  private static boolean allowedUnescaped(
      @Nonnull String name, @Nonnull Set<String> fragments, @Nonnull String template) {
    if (name.isEmpty() || ".".equals(name)) {
      return false;
    }
    if (SearchTemplateContextVariables.STRUCTURED_QUERY.equals(name)) {
      return true;
    }
    if (name.endsWith("Json") && SearchTemplateContextVariables.isContextName(name)) {
      return true;
    }
    if (fragments.contains(name)) {
      return true;
    }
    // A parent opens {{#name}} and splices {{&name}}. A child fills that fragment.
    // A context name is not a fragment, so {{&query}} stays rejected.
    return !SearchTemplateContextVariables.isContextName(name) && declaresSection(template, name);
  }

  private static boolean declaresSection(@Nonnull String template, @Nonnull String name) {
    Matcher matcher = SECTION.matcher(template);
    while (matcher.find()) {
      if (name.equals(matcher.group(1))) {
        return true;
      }
    }
    return false;
  }

  private static int skipSpace(@Nonnull String template, int cursor) {
    while (cursor < template.length()) {
      char c = template.charAt(cursor);
      if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
        return cursor;
      }
      cursor++;
    }
    return cursor;
  }

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
