package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.search.hybrid.HybridCandidateMerger;
import com.linkedin.metadata.search.hybrid.HybridLexicalScoreNormalizer;
import com.linkedin.metadata.search.hybrid.HybridRerankPlan;
import com.linkedin.metadata.search.hybrid.HybridScoreCombiner;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The rerank a keywordRerank rung runs over its keyword window: the rows whose entity type passes
 * the entity gates, the rung's knn blend, and its floor. When it cannot run, the search keeps
 * keyword order and reports one of the bounded reasons here.
 */
public final class SearchTemplateKeywordRerank {
  /** Changes when the rerank orders a window differently, so cached results are not shared. */
  public static final int EXECUTION_VERSION = 1;

  public static final String APPLIED = "applied";
  public static final String KEPT_KEYWORD_ORDER = "keptKeywordOrder";

  public static final String HYBRID_READ_OFF = "hybridReadOff";
  public static final String SEMANTIC_READ_OFF = "semanticReadOff";
  public static final String NO_DOCUMENT = "noDocument";

  /** Fewer than two rows in the window can be blended. */
  public static final String NO_ELIGIBLE_ROWS = "noEligibleRows";

  /** The resolved profile is not the one the query embedding service embeds. */
  public static final String PROFILE_NOT_EMBEDDABLE = "profileNotEmbeddable";

  public static final String REJECTED = "rejected";
  public static final String FAILED = "failed";
  public static final String TIMEOUT = "timeout";
  public static final String PAUSED = "paused";

  /** The request does not rank a relevance window, e.g. a count or a match-all query. */
  public static final String NOT_APPLICABLE = "notApplicable";

  public static final String MIN_SCORE_DEPRECATION =
      "SearchFlags.minScore is deprecated as a keywordRerank blend gate; use knn.blendFloor";

  private SearchTemplateKeywordRerank() {}

  /**
   * Why the rung keeps keyword order before any row is scored, or null: no requested type passes
   * the entity gates, or a profile they resolve is not the one the query embedding service embeds.
   */
  @Nullable
  public static String keptReason(
      @Nonnull MergedSearchTemplate template,
      @Nonnull SearchTemplateGates gates,
      @Nonnull Collection<String> entityTypes,
      @Nullable String system,
      @Nonnull String modelEmbeddingKey) {
    Set<String> profiles = gates.joinedProfileIds(template, entityTypes, system);
    if (profiles.isEmpty()) {
      return NO_ELIGIBLE_ROWS;
    }
    return profiles.equals(Set.of(modelEmbeddingKey)) ? null : PROFILE_NOT_EMBEDDABLE;
  }

  /** The rerank of the rung's window, for a rung {@link #keptReason} lets run. */
  @Nonnull
  public static HybridRerankPlan plan(
      @Nonnull MergedSearchTemplate template,
      @Nonnull SearchTemplateGates gates,
      @Nonnull Collection<String> entityTypes,
      @Nullable String system,
      @Nullable Float minScore) {
    Set<String> eligible = new LinkedHashSet<>();
    gates
        .entityGates(template, entityTypes, system)
        .forEach(
            (entityType, reason) -> {
              if (reason == null) {
                eligible.add(entityType);
              }
            });
    SearchTemplateRerankKnn knn =
        SearchTemplateJson.parseRerankKnn(template.getKnn() == null ? "{}" : template.getKnn());
    HybridCandidateMerger merger =
        new HybridCandidateMerger(
            new HybridLexicalScoreNormalizer(
                knn.lexicalInputMin(), knn.lexicalInputMax(), knn.lexicalSteepness()),
            new HybridScoreCombiner(knn.vectorWeight()));
    return new HybridRerankPlan(eligible, merger, floor(knn.blendFloor(), minScore));
  }

  // Both are inclusive gates on the same engine [0, 1] score and a row must pass both, so the
  // higher one decides.
  @Nullable
  static Double floor(@Nullable Double blendFloor, @Nullable Float minScore) {
    if (minScore == null) {
      return blendFloor;
    }
    return blendFloor == null ? (double) minScore : Math.max(blendFloor, minScore);
  }
}
