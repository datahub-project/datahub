package com.linkedin.metadata.search.hybrid;

import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * What one rerank of the keyword window scores.
 *
 * @param eligibleEntityNames entity types whose rows may move; every other row keeps its place
 * @param merger the blend of lexical and vector scores, or null for the reranker's own
 * @param blendFloor inclusive minimum engine [0, 1] vector score a row needs to be blended, or null
 *     for none. A row below it keeps its keyword place and is not dropped.
 */
public record HybridRerankPlan(
    @Nonnull Set<String> eligibleEntityNames,
    @Nullable HybridCandidateMerger merger,
    @Nullable Double blendFloor) {}
