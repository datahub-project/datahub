package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.search.template.SearchTemplateRanking;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Stage 2 and Stage 3 inputs for one post-process invocation. */
public record SearchTemplateRankingInput(
    @Nullable SearchTemplateRanking ranking,
    @Nonnull SearchTemplateCandidates candidates,
    @Nonnull String normalizedQuery,
    boolean includeExplain,
    boolean rescoreEnabled,
    @Nonnull String templateUrn) {}
