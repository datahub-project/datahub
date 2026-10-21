package com.linkedin.metadata.search.elasticsearch.query.template;

import javax.annotation.Nonnull;

/** Parsed {@code knn} section. Weights blend the keyword score with the vector score. */
public record SearchTemplateKnn(
    @Nonnull String field, int k, int numCandidates, float keywordWeight, float vectorWeight) {}
