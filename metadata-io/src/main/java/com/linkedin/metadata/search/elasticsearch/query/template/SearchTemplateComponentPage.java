package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Page envelope a component returns. POST_PROCESS keeps the engine total and facets. */
public record SearchTemplateComponentPage(
    @Nonnull List<SearchEntity> hits,
    long total,
    @Nullable SearchResultMetadata metadata,
    @Nonnull List<String> matchedFields,
    boolean partial,
    boolean shardFailed,
    @Nullable String explanation) {}
