package com.linkedin.metadata.search.hybrid;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/** Hybrid retrieval candidate with lexical, vector, and combined scores. */
public record HybridCandidate(
    @Nonnull Urn entity,
    double scaledLexicalScore,
    double vectorScore,
    double normalizedLexicalScore,
    double normalizedVectorScore,
    double combinedScore) {}
