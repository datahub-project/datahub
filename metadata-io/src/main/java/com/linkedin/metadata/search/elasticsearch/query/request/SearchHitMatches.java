package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.MatchEvidence;
import java.util.List;
import javax.annotation.Nonnull;

/** Operational matched fields, and the explain evidence that is attached only on request. */
public record SearchHitMatches(
    @Nonnull List<MatchedField> matched, @Nonnull List<MatchEvidence> explain) {
  public static final SearchHitMatches EMPTY = new SearchHitMatches(List.of(), List.of());
}
