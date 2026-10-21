package com.linkedin.metadata.search.elasticsearch.query.template;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import com.linkedin.search.template.SearchTemplateRoute;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Compiles route patterns once and matches them against a trimmed query. */
public final class SearchTemplateRoutes {
  /** Compiled RE2 patterns. The stored-expression length limit is maxRoutePatternLength. */
  private static final Cache<String, Pattern> PATTERNS =
      Caffeine.newBuilder().maximumSize(256).build();

  private SearchTemplateRoutes() {}

  public record Compiled(@Nonnull Pattern pattern, @Nonnull String target) {}

  @Nonnull
  public static List<Compiled> compile(
      @Nullable List<SearchTemplateRoute> routes, int maxRoutes, int maxPatternLength) {
    if (maxRoutes <= 0 || maxPatternLength <= 0) {
      throw new IllegalArgumentException(
          "elasticsearch.search.v3.templates maxRoutes and maxRoutePatternLength must be positive");
    }
    if (routes == null || routes.isEmpty()) {
      return List.of();
    }
    if (routes.size() > maxRoutes) {
      throw new SearchTemplateException(
          "Search template has " + routes.size() + " routes and the limit is " + maxRoutes);
    }
    List<Compiled> compiled = new ArrayList<>(routes.size());
    for (SearchTemplateRoute route : routes) {
      if (!route.hasQueryRegex()
          || route.getQueryRegex() == null
          || route.getQueryRegex().isBlank()) {
        throw new SearchTemplateException("Search template route pattern is empty");
      }
      if (route.getTemplate() == null) {
        throw new SearchTemplateException("Search template route target is missing");
      }
      compiled.add(
          new Compiled(
              pattern(route.getQueryRegex(), maxPatternLength), route.getTemplate().toString()));
    }
    return List.copyOf(compiled);
  }

  @Nonnull
  public static Pattern pattern(@Nonnull String regex, int maxPatternLength) {
    if (maxPatternLength <= 0) {
      throw new IllegalArgumentException(
          "elasticsearch.search.v3.templates.maxRoutePatternLength must be positive");
    }
    if (regex.length() > maxPatternLength) {
      throw new SearchTemplateException(
          "Search template route pattern is longer than " + maxPatternLength);
    }
    try {
      return PATTERNS.get(regex, SearchTemplateRoutes::compilePattern);
    } catch (RuntimeException e) {
      if (e instanceof SearchTemplateException || e.getCause() instanceof SearchTemplateException) {
        throw e instanceof SearchTemplateException
            ? (SearchTemplateException) e
            : (SearchTemplateException) e.getCause();
      }
      throw e;
    }
  }

  public record Decision(@Nullable String target, boolean usedDefault) {}

  @Nullable
  public static String target(@Nonnull MergedSearchTemplate template, @Nullable String query) {
    return decide(template, query).target();
  }

  /** {@code usedDefault} is true only when a pattern missed and a default route was taken. */
  @Nonnull
  public static Decision decide(@Nonnull MergedSearchTemplate template, @Nullable String query) {
    String text = query == null ? "" : query.trim();
    for (Compiled route : template.getRoutes()) {
      if (route.pattern().matcher(text).matches()) {
        return new Decision(route.target(), false);
      }
    }
    String fallback = template.getDefaultRoute();
    return new Decision(fallback, fallback != null);
  }

  @Nonnull
  private static Pattern compilePattern(@Nonnull String regex) {
    try {
      return Pattern.compile(regex);
    } catch (PatternSyntaxException e) {
      throw new SearchTemplateException("Search template route pattern is not valid RE2");
    }
  }
}
