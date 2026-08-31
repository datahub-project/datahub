package com.linkedin.metadata.timeline;

import static org.testng.Assert.*;

import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.lang.reflect.Method;
import java.util.*;
import org.testng.annotations.Test;

/**
 * Tests for {@link TimelineServiceImpl#combineTransactionsByTimestamp}. This private method merges
 * ChangeTransactions that share the same timestamp. It must handle unmodifiable lists in
 * changeEvents (e.g. from {@code Collections.singletonList()} used in the error-handling path).
 */
public class CombineTransactionsByTimestampTest {

  /**
   * When two ChangeTransactions share the same timestamp and the first has an unmodifiable
   * changeEvents list (e.g. Collections.singletonList), combineTransactionsByTimestamp must not
   * throw UnsupportedOperationException.
   */
  @Test
  public void testCombineTransactionsWithUnmodifiableChangeEventsList() throws Exception {
    long sharedTimestamp = 1000L;

    // First transaction uses an unmodifiable list (mirrors the error-handling path at line 622)
    ChangeEvent event1 =
        ChangeEvent.builder()
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.ADD)
            .description("Tag added")
            .semVerChange(SemanticChangeType.MINOR)
            .build();
    ChangeTransaction tx1 =
        ChangeTransaction.builder()
            .timestamp(sharedTimestamp)
            .changeEvents(Collections.singletonList(event1))
            .semVerChange(SemanticChangeType.MINOR)
            .semVer("1.0.0")
            .build();

    // Second transaction uses a normal mutable list
    ChangeEvent event2 =
        ChangeEvent.builder()
            .category(ChangeCategory.OWNERSHIP)
            .operation(ChangeOperation.ADD)
            .description("Owner added")
            .semVerChange(SemanticChangeType.MINOR)
            .build();
    ChangeTransaction tx2 =
        ChangeTransaction.builder()
            .timestamp(sharedTimestamp)
            .changeEvents(new ArrayList<>(List.of(event2)))
            .semVerChange(SemanticChangeType.MINOR)
            .semVer("1.0.0")
            .build();

    List<ChangeTransaction> transactions = new ArrayList<>(List.of(tx1, tx2));
    Map<Long, SortedMap<String, Long>> timestampVersionCache = new HashMap<>();

    // Invoke the private method via reflection
    Method method =
        TimelineServiceImpl.class.getDeclaredMethod(
            "combineTransactionsByTimestamp", List.class, Map.class);
    method.setAccessible(true);

    // This should NOT throw UnsupportedOperationException
    @SuppressWarnings("unchecked")
    List<ChangeTransaction> result =
        (List<ChangeTransaction>)
            method.invoke(createMinimalInstance(), transactions, timestampVersionCache);

    assertEquals(result.size(), 1, "Two transactions with same timestamp should merge into one");
    assertEquals(
        result.get(0).getChangeEvents().size(),
        2,
        "Merged transaction should contain both change events");
  }

  /**
   * Verify that combining transactions with mutable lists still works (regression guard for the
   * normal case).
   */
  @Test
  public void testCombineTransactionsWithMutableLists() throws Exception {
    long sharedTimestamp = 2000L;

    ChangeEvent event1 =
        ChangeEvent.builder()
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.ADD)
            .description("Tag added")
            .semVerChange(SemanticChangeType.MINOR)
            .build();
    ChangeEvent event2 =
        ChangeEvent.builder()
            .category(ChangeCategory.OWNERSHIP)
            .operation(ChangeOperation.REMOVE)
            .description("Owner removed")
            .semVerChange(SemanticChangeType.MAJOR)
            .build();

    ChangeTransaction tx1 =
        ChangeTransaction.builder()
            .timestamp(sharedTimestamp)
            .changeEvents(new ArrayList<>(List.of(event1)))
            .semVerChange(SemanticChangeType.MINOR)
            .semVer("1.0.0")
            .build();
    ChangeTransaction tx2 =
        ChangeTransaction.builder()
            .timestamp(sharedTimestamp)
            .changeEvents(new ArrayList<>(List.of(event2)))
            .semVerChange(SemanticChangeType.MAJOR)
            .semVer("2.0.0")
            .build();

    List<ChangeTransaction> transactions = new ArrayList<>(List.of(tx1, tx2));
    Map<Long, SortedMap<String, Long>> timestampVersionCache = new HashMap<>();

    Method method =
        TimelineServiceImpl.class.getDeclaredMethod(
            "combineTransactionsByTimestamp", List.class, Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<ChangeTransaction> result =
        (List<ChangeTransaction>)
            method.invoke(createMinimalInstance(), transactions, timestampVersionCache);

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getChangeEvents().size(), 2);
    // The higher semantic change type should win
    assertEquals(result.get(0).getSemVerChange(), SemanticChangeType.MAJOR);
  }

  /**
   * Creates a TimelineServiceImpl with null dependencies — sufficient for testing
   * combineTransactionsByTimestamp which doesn't use any instance fields.
   */
  private TimelineServiceImpl createMinimalInstance() {
    return new TimelineServiceImpl(null, null);
  }
}
