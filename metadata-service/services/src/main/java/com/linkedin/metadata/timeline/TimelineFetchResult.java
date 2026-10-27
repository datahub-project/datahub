package com.linkedin.metadata.timeline;

import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/**
 * Result of a multi-URN timeline fetch.
 *
 * <p>{@link #getTransactions()} is the merged, oldest-first stream. {@link #getSkippedUrnCount()}
 * is the number of input URNs whose timelines could not be fetched — typically because the caller
 * lacks view permission, the URN was deleted between resolve and fetch, or the underlying store
 * threw an error. Used by the UI to warn the user that the merged view may be partial.
 */
@Value
@Builder
public class TimelineFetchResult {
  @Nonnull List<ChangeTransaction> transactions;
  int skippedUrnCount;
}
