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

  /**
   * Reachable rungs and, when a body references them, the viewer. Rerank is appended separately.
   */
  @Nullable private String cacheIdentity;

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

  public void setExecutionCapabilities(boolean capabilityScroll, boolean documentRerank) {
    this.capabilityScroll = capabilityScroll;
    this.documentRerank = documentRerank;
  }

  public void setCacheIdentity(@Nonnull String cacheIdentity) {
    this.cacheIdentity = cacheIdentity;
  }

  /**
   * Every reachable rung, and the viewer when a body references one. Rerank is armed after the
   * first batch, so it stays off this key.
   */
  @Nonnull
  public String cacheKeyComponent() {
    return cacheIdentity == null ? urn + "@" + lastModified : cacheIdentity;
  }
}
