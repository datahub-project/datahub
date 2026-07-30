package com.linkedin.metadata.timeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

public interface TimelineService {

  int DEFAULT_MAX_CHANGE_TRANSACTIONS = 100;

  List<ChangeTransaction> getTimeline(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull Set<ChangeCategory> elements,
      long startMillis,
      long endMillis,
      String startVersionStamp,
      String endVersionStamp,
      boolean rawDiffRequested)
      throws JsonProcessingException;

  List<ChangeTransaction> getTimeline(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull Set<ChangeCategory> elements,
      int maxChangeTransactions,
      boolean rawDiffRequested);

  /**
   * Fetches and merges timelines for multiple URNs into one unified, oldest-first stream.
   *
   * <p>This is the low-level primitive used by the {@code includeVersionSet} feature: the caller
   * (typically {@code GetTimelineResolver}) resolves the sibling version URNs using the
   * EntityClient and then hands the full list to this method. Each URN is queried independently and
   * the results are merged and sorted by {@code timestampMillis} (with the actor as a stable
   * tie-breaker).
   *
   * <p>Per-URN failures (auth, deletion race, store errors) are skipped rather than thrown so a
   * partial result can still be returned. The number of skipped URNs is reported on the result so
   * the caller can surface a "view may be partial" warning to the user.
   *
   * @param urns All URNs to include (typically: current version + all siblings in the VersionSet).
   * @param elements Change categories to include.
   * @param rawDiffRequested Whether to include raw JSON diffs in the response.
   */
  @Nonnull
  TimelineFetchResult getTimelineForUrns(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> urns,
      @Nonnull Set<ChangeCategory> elements,
      boolean rawDiffRequested);
}
