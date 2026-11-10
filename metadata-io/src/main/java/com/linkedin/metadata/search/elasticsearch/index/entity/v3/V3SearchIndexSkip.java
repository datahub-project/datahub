package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.metadata.aspect.batch.MCLItem;
import javax.annotation.Nonnull;

/**
 * Decides whether one change event is left out of the V3 search document. OSS registers none. An
 * extension registers a bean when a class of events must not be indexed.
 */
public interface V3SearchIndexSkip {
  boolean skip(@Nonnull MCLItem event);
}
