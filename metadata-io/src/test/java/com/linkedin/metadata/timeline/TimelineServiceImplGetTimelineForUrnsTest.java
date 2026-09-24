package com.linkedin.metadata.timeline;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Focused unit test for {@link TimelineServiceImpl#getTimelineForUrns}. The per-URN {@link
 * TimelineServiceImpl#getTimeline} call is stubbed via a Mockito spy so we can exercise the merge,
 * sort, and skip-counting logic without a backing store.
 */
public class TimelineServiceImplGetTimelineForUrnsTest {

  private static final Urn URN_A = UrnUtils.getUrn("urn:li:glossaryTerm:example.termA");
  private static final Urn URN_B = UrnUtils.getUrn("urn:li:glossaryTerm:example.termB");
  private static final Urn URN_C = UrnUtils.getUrn("urn:li:glossaryTerm:example.termC");
  private static final Set<ChangeCategory> CATEGORIES = Set.of(ChangeCategory.VERSIONING);

  private static TimelineServiceImpl newSpy() {
    return spy(new TimelineServiceImpl(mock(AspectDao.class), mock(EntityRegistry.class)));
  }

  private static ChangeTransaction txn(long timestamp, String actor) {
    return ChangeTransaction.builder()
        .timestamp(timestamp)
        .actor(actor)
        .changeEvents(Collections.emptyList())
        .build();
  }

  @Test
  public void testMergesAndSortsByTimestamp() {
    TimelineServiceImpl service = newSpy();
    OperationContext opContext = mock(OperationContext.class);

    // URN_A's events are newer than URN_B's; the merged stream must be oldest-first.
    doReturn(List.of(txn(300L, "urn:li:corpuser:a")))
        .when(service)
        .getTimeline(any(), eq(URN_A), any(), anyInt(), anyBoolean());
    doReturn(List.of(txn(100L, "urn:li:corpuser:b")))
        .when(service)
        .getTimeline(any(), eq(URN_B), any(), anyInt(), anyBoolean());

    TimelineFetchResult result =
        service.getTimelineForUrns(opContext, List.of(URN_A, URN_B), CATEGORIES, false);

    assertEquals(result.getSkippedUrnCount(), 0);
    List<ChangeTransaction> merged = result.getTransactions();
    assertEquals(merged.size(), 2);
    assertEquals(merged.get(0).getTimestamp(), 100L);
    assertEquals(merged.get(1).getTimestamp(), 300L);
  }

  @Test
  public void testActorTieBreakOnEqualTimestamp() {
    TimelineServiceImpl service = newSpy();
    OperationContext opContext = mock(OperationContext.class);

    doReturn(List.of(txn(100L, "urn:li:corpuser:zoe")))
        .when(service)
        .getTimeline(any(), eq(URN_A), any(), anyInt(), anyBoolean());
    doReturn(List.of(txn(100L, "urn:li:corpuser:amy")))
        .when(service)
        .getTimeline(any(), eq(URN_B), any(), anyInt(), anyBoolean());

    TimelineFetchResult result =
        service.getTimelineForUrns(opContext, List.of(URN_A, URN_B), CATEGORIES, false);

    List<ChangeTransaction> merged = result.getTransactions();
    assertEquals(merged.size(), 2);
    // Same timestamp → tie broken by actor ascending: "amy" before "zoe".
    assertEquals(merged.get(0).getActor(), "urn:li:corpuser:amy");
    assertEquals(merged.get(1).getActor(), "urn:li:corpuser:zoe");
  }

  @Test
  public void testPerUrnFailureIsSkippedNotFatal() {
    TimelineServiceImpl service = newSpy();
    OperationContext opContext = mock(OperationContext.class);

    doReturn(List.of(txn(100L, "urn:li:corpuser:a")))
        .when(service)
        .getTimeline(any(), eq(URN_A), any(), anyInt(), anyBoolean());
    doThrow(new RuntimeException("store unavailable"))
        .when(service)
        .getTimeline(any(), eq(URN_B), any(), anyInt(), anyBoolean());
    doReturn(List.of(txn(200L, "urn:li:corpuser:c")))
        .when(service)
        .getTimeline(any(), eq(URN_C), any(), anyInt(), anyBoolean());

    TimelineFetchResult result =
        service.getTimelineForUrns(opContext, List.of(URN_A, URN_B, URN_C), CATEGORIES, false);

    // URN_B failed → skipped, but URN_A and URN_C still merge.
    assertEquals(result.getSkippedUrnCount(), 1);
    assertEquals(result.getTransactions().size(), 2);
  }

  @Test
  public void testEmptyInputYieldsEmptyResult() {
    TimelineServiceImpl service = newSpy();

    TimelineFetchResult result =
        service.getTimelineForUrns(
            mock(OperationContext.class), Collections.emptyList(), CATEGORIES, false);

    assertEquals(result.getSkippedUrnCount(), 0);
    assertTrue(result.getTransactions().isEmpty());
  }
}
