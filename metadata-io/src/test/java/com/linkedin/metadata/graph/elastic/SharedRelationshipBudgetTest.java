package com.linkedin.metadata.graph.elastic;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

/**
 * Unit tests for the per-hop shared relationship budget helpers on {@link GraphQueryBaseDAO}. These
 * bound lineage traversal memory: without a shared budget each parallel slice self-capped to the
 * full remaining maxRelations, so the transient per-hop peak was {@code slices * maxRelations}. The
 * atomic reserve/return primitives replace a racy read-then-deduct so concurrent slices can never
 * collectively retain more than maxRelations.
 */
public class SharedRelationshipBudgetTest {

  @Test
  public void testNewSharedBudgetUnlimitedSentinelIsNull() {
    assertNull(GraphQueryBaseDAO.newSharedRelationshipBudget(0));
    assertNull(GraphQueryBaseDAO.newSharedRelationshipBudget(-1));
  }

  @Test
  public void testNewSharedBudgetSeedsCounter() {
    AtomicInteger budget = GraphQueryBaseDAO.newSharedRelationshipBudget(40000);
    assertEquals(budget.get(), 40000);
  }

  @Test
  public void testReserveUnlimitedGrantsFullWant() {
    // A null budget means unlimited: reserve the full requested amount, never capping.
    assertEquals(GraphQueryBaseDAO.reserveSharedBudget(null, 500), 500);
    // A non-positive want reserves nothing.
    assertEquals(GraphQueryBaseDAO.reserveSharedBudget(new AtomicInteger(10), 0), 0);
  }

  @Test
  public void testReserveGrantsUpToRemainingThenExhausts() {
    AtomicInteger b = new AtomicInteger(10);
    assertEquals(GraphQueryBaseDAO.reserveSharedBudget(b, 4), 4);
    assertEquals(b.get(), 6);
    assertEquals(GraphQueryBaseDAO.reserveSharedBudget(b, 100), 6); // capped to remaining
    assertEquals(b.get(), 0);
    assertEquals(GraphQueryBaseDAO.reserveSharedBudget(b, 5), 0); // exhausted
  }

  @Test
  public void testReserveNeverOverGrantsUnderConcurrency() throws Exception {
    // The core invariant behind the fix: N parallel slices reserving from one shared budget can
    // never collectively reserve more than maxRelations (no read-then-deduct overshoot), and the
    // atomic reservation loses no capacity either — the total handed out equals the budget exactly.
    int maxRelations = 40_000;
    int page = 137; // odd page size to exercise the remainder near exhaustion
    int slices = 8;
    AtomicInteger budget = new AtomicInteger(maxRelations);
    AtomicInteger totalReserved = new AtomicInteger(0);
    ExecutorService pool = Executors.newFixedThreadPool(slices);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < slices; i++) {
        futures.add(
            pool.submit(
                () -> {
                  int grant;
                  while ((grant = GraphQueryBaseDAO.reserveSharedBudget(budget, page)) > 0) {
                    totalReserved.addAndGet(grant);
                  }
                }));
      }
      for (Future<?> f : futures) {
        f.get(30, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(totalReserved.get(), maxRelations);
    assertEquals(budget.get(), 0);
  }

  @Test
  public void testStopSliceGuardStrictRejectsWhenExhausted() {
    // Reaching the cap in strict mode is an error, even at an exact page boundary where no
    // partial grant ever happened — matching the pre-shared-budget per-slice behavior.
    assertFalse(GraphQueryBaseDAO.stopSliceIfSharedBudgetExhausted(null, 100, 0, false));
    assertFalse(
        GraphQueryBaseDAO.stopSliceIfSharedBudgetExhausted(new AtomicInteger(1), 100, 0, false));
    try {
      GraphQueryBaseDAO.stopSliceIfSharedBudgetExhausted(new AtomicInteger(0), 100, 0, false);
      fail("Strict mode must reject when the shared budget is exhausted");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("maxRelations limit"));
    }
  }

  @Test
  public void testStopSliceGuardPartialStopsWhenExhausted() {
    assertTrue(
        GraphQueryBaseDAO.stopSliceIfSharedBudgetExhausted(new AtomicInteger(0), 100, 0, true));
    assertFalse(
        GraphQueryBaseDAO.stopSliceIfSharedBudgetExhausted(new AtomicInteger(5), 100, 0, true));
  }

  @Test
  public void testMarkPartialWhenBudgetExhausted() {
    // An exhausted budget means the hop was truncated at maxRelations; the fetch must be reported
    // partial directly, because the outer unique-entity limit check can miss the truncation when
    // cross-slice duplicates merge to fewer unique entities than relationships were retained.
    LineageSliceFetchResult fetch = new LineageSliceFetchResult(List.of(), false);
    assertTrue(
        GraphQueryBaseDAO.markPartialIfSharedBudgetExhausted(fetch, new AtomicInteger(0), true)
            .isPartial());
    // Budget remaining, unlimited, or strict mode (a slice rejects instead): unchanged.
    assertFalse(
        GraphQueryBaseDAO.markPartialIfSharedBudgetExhausted(fetch, new AtomicInteger(3), true)
            .isPartial());
    assertFalse(
        GraphQueryBaseDAO.markPartialIfSharedBudgetExhausted(fetch, null, true).isPartial());
    assertFalse(
        GraphQueryBaseDAO.markPartialIfSharedBudgetExhausted(fetch, new AtomicInteger(0), false)
            .isPartial());
  }
}
