package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Caller URN, otherwise the seed for the fulltext flag. The seed's routes then choose the template.
 * A miss with no default stays on the current template.
 */
public class SearchTemplateSelector {
  private final SearchTemplateConfiguration configuration;
  private final SearchTemplateLoader loader;

  public SearchTemplateSelector(
      @Nonnull SearchTemplateConfiguration configuration, @Nonnull SearchTemplateLoader loader) {
    this.configuration = configuration;
    this.loader = loader;
  }

  @Nonnull
  public ResolvedSearchTemplate select(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      boolean fulltext,
      @Nullable SearchClientShim.SearchEngineType engineType) {
    String requestDialect = SearchTemplateDialects.requestDialect(engineType);
    String engineKey = SearchTemplateDialects.engineKey(engineType);
    SearchFlags flags = opContext.getSearchContext().getSearchFlags();
    if (flags != null && flags.getSearchTemplate() != null) {
      return walk(opContext, query, requestDialect, flags.getSearchTemplate().toString());
    }
    String seed = fulltext ? "fulltext" : "structured";
    String surface = opContext.getSearchContext().getSearchSurface();
    Map<String, Object> surfaceSlots = surfaceSlots(surface, seed);
    Map<String, Object> globalSlots = configuration.getDefaults().get(seed);
    List<String> candidates = new ArrayList<>();
    add(candidates, urnAt(surfaceSlots, engineKey));
    add(candidates, urnAt(surfaceSlots, "default"));
    add(candidates, urnAt(globalSlots, engineKey));
    add(candidates, urnAt(globalSlots, "default"));
    for (String urn : candidates) {
      MergedSearchTemplate template = loader.find(opContext, urn);
      if (template != null
          && SearchTemplateDialects.matches(template.getDialect(), requestDialect)) {
        return walk(opContext, query, requestDialect, urn);
      }
    }
    throw new SearchTemplateException(
        "No search template for seed " + seed + " on dialect " + requestDialect);
  }

  @Nonnull
  private ResolvedSearchTemplate walk(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      @Nonnull String requestDialect,
      @Nonnull String urn) {
    MergedSearchTemplate current = loadMatching(opContext, requestDialect, urn);
    List<String> chain = new ArrayList<>();
    Set<String> visited = new HashSet<>();
    while (true) {
      if (!visited.add(current.getUrn())) {
        throw new SearchTemplateException("Search template route repeats " + current.getUrn());
      }
      if (visited.size() > SearchTemplateRoutes.MAX_ROUTE_HOPS) {
        throw new SearchTemplateException(
            "Search template route hop limit is " + SearchTemplateRoutes.MAX_ROUTE_HOPS);
      }
      chain.add(current.getUrn());
      String next = SearchTemplateRoutes.target(current, query);
      if (next == null) {
        ResolvedSearchTemplate resolved =
            new ResolvedSearchTemplate(current.getUrn(), current.getLastModified(), null, null);
        resolved.setRouteChain(chain);
        return resolved;
      }
      current = loadMatching(opContext, requestDialect, next);
    }
  }

  @Nonnull
  private MergedSearchTemplate loadMatching(
      @Nonnull OperationContext opContext, @Nonnull String requestDialect, @Nonnull String urn) {
    MergedSearchTemplate template = loader.load(opContext, urn);
    if (!SearchTemplateDialects.matches(template.getDialect(), requestDialect)) {
      throw new SearchTemplateException(
          "Search template "
              + urn
              + " is dialect "
              + template.getDialect()
              + " and this request is "
              + requestDialect);
    }
    return template;
  }

  @Nullable
  private Map<String, Object> surfaceSlots(@Nullable String surface, @Nonnull String seed) {
    if (surface == null || configuration.getSurfaces() == null) {
      return null;
    }
    Map<String, Map<String, Object>> intents = configuration.getSurfaces().get(surface);
    if (intents == null) {
      return null;
    }
    return intents.get(seed);
  }

  @Nullable
  private static String urnAt(@Nullable Map<String, Object> slots, @Nonnull String key) {
    if (slots == null) {
      return null;
    }
    Object value = slots.get(key);
    if (value instanceof String urn && !urn.isBlank()) {
      return urn;
    }
    return null;
  }

  private static void add(@Nonnull List<String> candidates, @Nullable String urn) {
    if (urn != null && !candidates.contains(urn)) {
      candidates.add(urn);
    }
  }
}
