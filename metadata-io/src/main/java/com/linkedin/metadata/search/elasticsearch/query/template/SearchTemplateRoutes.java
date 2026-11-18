package com.linkedin.metadata.search.elasticsearch.query.template;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import com.linkedin.search.template.SearchTemplateRoute;
import com.linkedin.search.template.SearchTemplateRouteMatch;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Compiles route patterns once and matches them against a trimmed query. */
public final class SearchTemplateRoutes {
  /** Compiled RE2 patterns. The stored-expression length limit is maxRoutePatternLength. */
  private static final Cache<String, Pattern> PATTERNS =
      Caffeine.newBuilder().maximumSize(256).build();

  private SearchTemplateRoutes() {}

  /** {@code match} is null when the route matches on its pattern alone. */
  public record Compiled(
      @Nonnull Pattern pattern, @Nonnull String target, @Nullable SearchTemplateRouteMatch match) {}

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
      SearchTemplateRouteMatch match = route.getMatch();
      if (match != null) {
        checkMatch(match);
      }
      compiled.add(
          new Compiled(
              pattern(route.getQueryRegex(), maxPatternLength),
              route.getTemplate().toString(),
              emptyMatch(match) ? null : match));
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

  /**
   * What a route's {@code match} conditions read: the full request entity set, the request surface,
   * and the set's live semantic eligibility.
   */
  public record Inputs(
      @Nonnull List<String> entityTypes,
      @Nullable String surface,
      @Nonnull SearchTemplateGates.Eligibility eligibility) {

    /** The inputs in a canonical form, for the result-cache identity. */
    @Nonnull
    public String identity() {
      return new TreeSet<>(entityTypes)
          + ";"
          + surface
          + ";"
          + eligibility.all()
          + ","
          + eligibility.any();
    }
  }

  /**
   * {@code conditional} is true when the route that fired has {@code match} conditions, so the
   * request inputs decided the target.
   */
  public record Decision(@Nullable String target, boolean usedDefault, boolean conditional) {}

  @Nullable
  public static String target(
      @Nonnull MergedSearchTemplate template, @Nullable String query, @Nonnull Inputs inputs) {
    return decide(template, query, inputs).target();
  }

  /**
   * The first route whose pattern matches and whose conditions all hold. A route whose conditions
   * miss does not stop the scan. {@code usedDefault} is true only when no route fired and a default
   * route was taken.
   */
  @Nonnull
  public static Decision decide(
      @Nonnull MergedSearchTemplate template, @Nullable String query, @Nonnull Inputs inputs) {
    String text = query == null ? "" : query.trim();
    for (Compiled route : template.getRoutes()) {
      if (!route.pattern().matcher(text).matches()) {
        continue;
      }
      if (route.match() == null) {
        return new Decision(route.target(), false, false);
      }
      if (holds(route.match(), inputs)) {
        return new Decision(route.target(), false, true);
      }
    }
    String fallback = template.getDefaultRoute();
    return new Decision(fallback, fallback != null, false);
  }

  /**
   * Every set condition holds. Booleans compare exactly, so {@code semanticEligible: false} needs a
   * request that is not all enabled. A condition this release does not know fails the search
   * instead of being read as met.
   */
  private static boolean holds(@Nonnull SearchTemplateRouteMatch match, @Nonnull Inputs inputs) {
    for (String key : match.data().keySet()) {
      if (match.schema().getField(key) == null) {
        throw new SearchTemplateException(
            "Search template section is not supported: route match." + key);
      }
    }
    return (!match.hasEntityTypes() || match.getEntityTypes().containsAll(inputs.entityTypes()))
        && (!match.hasSurfaces()
            || (inputs.surface() != null && match.getSurfaces().contains(inputs.surface())))
        && (!match.hasSemanticEligible()
            || match.isSemanticEligible() == inputs.eligibility().all())
        && (!match.hasAnySemanticEligible()
            || match.isAnySemanticEligible() == inputs.eligibility().any());
  }

  /** Rejects conditions that are invalid in every build. */
  private static void checkMatch(@Nonnull SearchTemplateRouteMatch match) {
    names("route match.entityTypes", match.getEntityTypes());
    names("route match.surfaces", match.getSurfaces());
  }

  private static boolean emptyMatch(@Nullable SearchTemplateRouteMatch match) {
    // Judged from the stored data, so a condition this release does not know still fails closed.
    return match == null || match.data().isEmpty();
  }

  /** Each name is non-blank and listed once. */
  public static void names(@Nonnull String field, @Nullable List<String> values) {
    if (values == null) {
      return;
    }
    Set<String> seen = new HashSet<>();
    for (String value : values) {
      if (value == null || value.isBlank()) {
        throw new SearchTemplateException("Search template " + field + " has an empty name");
      }
      if (!seen.add(value)) {
        throw new SearchTemplateException(
            "Search template " + field + " names " + value + " more than once");
      }
    }
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
