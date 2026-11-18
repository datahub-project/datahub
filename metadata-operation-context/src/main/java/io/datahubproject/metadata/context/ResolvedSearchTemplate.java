package io.datahubproject.metadata.context;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Template chosen for one search invocation. Highlight field names are filled when the template is
 * rendered and are not part of the result-cache identity.
 */
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ResolvedSearchTemplate {
  @EqualsAndHashCode.Include @Nonnull private final String urn;
  @EqualsAndHashCode.Include private final long lastModified;
  @Nullable private final String intent;
  @Nullable private final String subIntent;
  @Nonnull private final List<String> highlightFields = new ArrayList<>();

  /** True when this request will run the template's kNN leg. Part of the result-cache identity. */
  private boolean knnApplied;

  /**
   * URN of the served rung when it declares {@code keywordRerank}, else null. That rung owns the
   * rerank of its keyword window, so the default entity hybrid reranker does not also run on it.
   */
  @Nullable private String templateRerank;

  /**
   * True when the served rung's own sort ordered the request, so its window is not in relevance
   * order. Not part of the result-cache identity.
   */
  private boolean templateSortApplied;

  /** Templates visited by the route walk. Not part of the result-cache identity. */
  @Nonnull private final List<String> routeChain = new ArrayList<>();

  /**
   * Engine slot that supplied the seed ({@code default}, {@code elasticsearch8}, {@code
   * opensearch3}, …) or {@code caller} when the request named a template. Not part of the
   * result-cache identity.
   */
  @Nullable private String engineSlot;

  private float keywordWeight = 1f;
  private float vectorWeight = 1f;
  private int knnK = 100;
  private boolean capabilityScroll;
  private boolean documentRerank;

  /** Requested entity types, for the per-rung embedding profile checks. */
  @Nullable private List<String> entityTypes;

  /** Registry name of the engine serving this search, or null when it is not known. */
  @Nullable private String searchSystem;

  /**
   * Reachable rungs and, when a body references them, the viewer. Rerank is appended separately.
   */
  @Nullable private String cacheIdentity;

  /** A rung this search may reach retrieves by vector. Set at bind. */
  private boolean vectorReachable;

  /**
   * The page the caller asked for, when the search runs in batches. A vector rung sizes and judges
   * its window from it, so it is part of the cache identity when a vector rung is reachable.
   */
  @Nullable private Integer requestedFrom;

  @Nullable private Integer requestedSize;

  /**
   * A vector rung failed and the ladder served its next rung, so the result is not cached and the
   * next search tries the vector rung again. Not part of the cache identity.
   */
  private boolean skipResultCache;

  public ResolvedSearchTemplate(
      @Nonnull String urn, long lastModified, @Nullable String intent, @Nullable String subIntent) {
    this.urn = urn;
    this.lastModified = lastModified;
    this.intent = intent;
    this.subIntent = subIntent;
  }

  public void setHighlightFields(@Nonnull List<String> fields) {
    highlightFields.clear();
    highlightFields.addAll(fields);
  }

  public void setRouteChain(@Nonnull List<String> chain) {
    routeChain.clear();
    routeChain.addAll(chain);
  }

  public void setEngineSlot(@Nullable String engineSlot) {
    this.engineSlot = engineSlot;
  }

  public void setKnn(boolean applied, float keywordWeight, float vectorWeight, int knnK) {
    this.knnApplied = applied;
    this.keywordWeight = keywordWeight;
    this.vectorWeight = vectorWeight;
    this.knnK = knnK;
  }

  public void setTemplateRerank(@Nullable String servedUrn) {
    this.templateRerank = servedUrn;
  }

  public void setTemplateSortApplied(boolean applied) {
    this.templateSortApplied = applied;
  }

  public void setExecutionCapabilities(
      boolean capabilityScroll,
      boolean documentRerank,
      @Nullable List<String> entityTypes,
      @Nullable String searchSystem) {
    this.capabilityScroll = capabilityScroll;
    this.documentRerank = documentRerank;
    this.entityTypes = entityTypes == null ? null : List.copyOf(entityTypes);
    this.searchSystem = searchSystem;
  }

  public void setCacheIdentity(@Nonnull String cacheIdentity) {
    this.cacheIdentity = cacheIdentity;
  }

  public void setVectorReachable(boolean vectorReachable) {
    this.vectorReachable = vectorReachable;
  }

  public void setRequestedPage(int from, int size) {
    this.requestedFrom = from;
    this.requestedSize = size;
  }

  public void setSkipResultCache(boolean skipResultCache) {
    this.skipResultCache = skipResultCache;
  }

  /**
   * Every reachable rung, and the viewer when a body references one. Rerank is armed after the
   * first batch, so it stays off this key.
   */
  @Nonnull
  public String cacheKeyComponent() {
    String identity = cacheIdentity == null ? urn + "@" + lastModified : cacheIdentity;
    return vectorReachable && requestedFrom != null
        ? identity + "|page=" + requestedFrom + ":" + requestedSize
        : identity;
  }
}
