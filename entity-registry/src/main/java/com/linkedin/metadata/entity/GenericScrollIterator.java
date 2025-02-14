package com.linkedin.metadata.entity;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Builder;

/**
 * Fetches pages of structured properties which have been applied to an entity urn with a specified
 * filter
 */
@Builder
public class GenericScrollIterator implements Iterator<ScrollResult> {
  @Nonnull private final Filter filter;
  @Nonnull private final List<String> entities;
  @Nonnull private final SearchRetriever searchRetriever;
  private int count;
  @Builder.Default private String scrollId = null;
  @Builder.Default private boolean started = false;

  @Override
  public boolean hasNext() {
    return !started || scrollId != null;
  }

  @Override
  public ScrollResult next() {
    started = true;
    ScrollResult result = searchRetriever.scroll(entities, filter, scrollId, count);
    scrollId = result.getScrollId();
    return result;
  }
}
