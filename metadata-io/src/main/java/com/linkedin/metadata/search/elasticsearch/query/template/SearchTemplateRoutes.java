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
  public static final int MAX_ROUTE_PATTERN_LENGTH = 256;
  public static final int MAX_ROUTES = 32;
  public static final int MAX_ROUTE_HOPS = 5;

  private static final Cache<String, Pattern> PATTERNS =
      Caffeine.newBuilder().maximumSize(256).build();

  private SearchTemplateRoutes() {}

  public record Compiled(@Nonnull Pattern pattern, @Nonnull String target) {}

  @Nonnull
  public static List<Compiled> compile(@Nullable List<SearchTemplateRoute> routes) {
    if (routes == null || routes.isEmpty()) {
      return List.of();
    }
    if (routes.size() > MAX_ROUTES) {
      throw new SearchTemplateException(
          "Search template has " + routes.size() + " routes and the limit is " + MAX_ROUTES);
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
      compiled.add(new Compiled(pattern(route.getQueryRegex()), route.getTemplate().toString()));
    }
    return List.copyOf(compiled);
  }

  @Nonnull
  public static Pattern pattern(@Nonnull String regex) {
    if (regex.length() > MAX_ROUTE_PATTERN_LENGTH) {
      throw new SearchTemplateException(
          "Search template route pattern is longer than " + MAX_ROUTE_PATTERN_LENGTH);
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

  @Nullable
  public static String target(@Nonnull MergedSearchTemplate template, @Nullable String query) {
    String text = query == null ? "" : query.trim();
    for (Compiled route : template.getRoutes()) {
      if (route.pattern().matcher(text).matches()) {
        return route.target();
      }
    }
    return template.getDefaultRoute();
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
