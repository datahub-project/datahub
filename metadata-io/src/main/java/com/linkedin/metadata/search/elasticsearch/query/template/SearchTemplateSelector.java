package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.datahub.authorization.AuthorizationResult;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.metadata.exception.ActorAccessException;
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
    if (configuration.getMaxRouteHops() <= 0) {
      throw new IllegalArgumentException(
          "elasticsearch.search.v3.templates.maxRouteHops must be positive");
    }
    this.configuration = configuration;
    this.loader = loader;
  }

  @Nonnull
  public ResolvedSearchTemplate select(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      boolean fulltext,
      @Nullable SearchClientShim.SearchEngineType engineType) {
    try {
      ResolvedSearchTemplate resolved = selectUnchecked(opContext, query, fulltext, engineType);
      SearchTemplateMetrics.selected(opContext, resolved.getUrn(), resolved.getEngineSlot());
      return resolved;
    } catch (RuntimeException e) {
      SearchTemplateMetrics.renderFailure(opContext, "select");
      throw e;
    }
  }

  @Nonnull
  private ResolvedSearchTemplate selectUnchecked(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      boolean fulltext,
      @Nullable SearchClientShim.SearchEngineType engineType) {
    String requestDialect = SearchTemplateDialects.requestDialect(engineType);
    String engineKey = SearchTemplateDialects.engineKey(engineType);
    SearchFlags flags = opContext.getSearchContext().getSearchFlags();
    if (flags != null && flags.getSearchTemplate() != null) {
      boolean systemActor =
          opContext.getSessionActorContext().isSystemAuth()
              || SYSTEM_ACTOR.equals(opContext.getSessionActorContext().getActorUrn().toString());
      AuthorizationResult authorization =
          systemActor
              ? null
              : opContext.authorize(
                  PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.getType(), null);
      if (!systemActor && authorization.getType() != AuthorizationResult.Type.ALLOW) {
        throw new ActorAccessException(
            "Selecting a search template requires MANAGE_SYSTEM_OPERATIONS");
      }
      ResolvedSearchTemplate resolved =
          walk(opContext, query, requestDialect, flags.getSearchTemplate().toString());
      resolved.setEngineSlot(SearchTemplateMetrics.SLOT_CALLER);
      return resolved;
    }
    String seed = fulltext ? "fulltext" : "structured";
    String surface = opContext.getSearchContext().getSearchSurface();
    Map<String, Object> surfaceSlots = surfaceSlots(surface, seed);
    Map<String, Object> globalSlots = configuration.getDefaults().get(seed);
    List<Candidate> candidates = new ArrayList<>();
    add(candidates, urnAt(surfaceSlots, engineKey), engineKey);
    add(candidates, urnAt(surfaceSlots, "default"), "default");
    add(candidates, urnAt(globalSlots, engineKey), engineKey);
    add(candidates, urnAt(globalSlots, "default"), "default");
    for (Candidate candidate : candidates) {
      MergedSearchTemplate template = loader.find(opContext, candidate.urn());
      if (template != null
          && SearchTemplateDialects.matches(template.getDialect(), requestDialect)) {
        ResolvedSearchTemplate resolved = walk(opContext, query, requestDialect, candidate.urn());
        resolved.setEngineSlot(candidate.slot());
        return resolved;
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
      if (visited.size() > configuration.getMaxRouteHops()) {
        throw new SearchTemplateException(
            "Search template route hop limit is " + configuration.getMaxRouteHops());
      }
      chain.add(current.getUrn());
      SearchTemplateRoutes.Decision decision = SearchTemplateRoutes.decide(current, query);
      if (decision.target() == null
          && query.startsWith(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX)) {
        decision =
            SearchTemplateRoutes.decide(
                current, query.substring(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX.length()));
      }
      if (decision.usedDefault()) {
        SearchTemplateMetrics.fallback(opContext, "route");
      }
      if (decision.target() == null) {
        ResolvedSearchTemplate resolved =
            new ResolvedSearchTemplate(current.getUrn(), current.getLastModified(), null, null);
        resolved.setRouteChain(chain);
        SearchTemplateMetrics.routeDepth(opContext, chain.size());
        return resolved;
      }
      current = loadMatching(opContext, requestDialect, decision.target());
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

  private static void add(
      @Nonnull List<Candidate> candidates, @Nullable String urn, @Nonnull String slot) {
    if (urn == null) {
      return;
    }
    for (Candidate candidate : candidates) {
      if (candidate.urn().equals(urn)) {
        return;
      }
    }
    candidates.add(new Candidate(urn, slot));
  }

  private record Candidate(@Nonnull String urn, @Nonnull String slot) {}
}
