package com.linkedin.metadata.graph.elastic;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

/**
 * Unit tests for the per-hop shared relationship budget helpers on {@link GraphQueryBaseDAO}. These
 * bound lineage traversal memory: without a shared budget each parallel slice self-capped to the
 * full remaining maxRelations, so the transient per-hop peak was {@code slices * maxRelations}.
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
  public void testNextPageSizeUnlimitedKeepsDefault() {
    assertEquals(GraphQueryBaseDAO.nextSharedPageSize(null, 5000), 5000);
  }

  @Test
  public void testNextPageSizeCapsToRemaining() {
    // Plenty of budget: full page. Near the limit: shrink so a fetch cannot overshoot by a page.
    assertEquals(GraphQueryBaseDAO.nextSharedPageSize(new AtomicInteger(40000), 5000), 5000);
    assertEquals(GraphQueryBaseDAO.nextSharedPageSize(new AtomicInteger(10), 5000), 10);
    // Never returns 0 (a zero-size page would spin without progress).
    assertEquals(GraphQueryBaseDAO.nextSharedPageSize(new AtomicInteger(0), 5000), 1);
  }

  @Test
  public void testDeductUnlimitedNeverExhausts() {
    assertFalse(GraphQueryBaseDAO.deductSharedBudgetAndCheckExhausted(null, 1_000_000));
  }

  @Test
  public void testDeductWithinBudget() {
    AtomicInteger budget = new AtomicInteger(10);
    assertFalse(GraphQueryBaseDAO.deductSharedBudgetAndCheckExhausted(budget, 4));
    assertEquals(budget.get(), 6);
  }

  @Test
  public void testDeductToExactlyZeroExhausts() {
    AtomicInteger budget = new AtomicInteger(6);
    assertTrue(GraphQueryBaseDAO.deductSharedBudgetAndCheckExhausted(budget, 6));
    assertEquals(budget.get(), 0);
  }

  @Test
  public void testDeductBeyondBudgetExhausts() {
    AtomicInteger budget = new AtomicInteger(3);
    // A page larger than the remaining budget drives it negative and reports exhaustion, so the
    // slice that consumed the last of the shared budget is the one that stops.
    assertTrue(GraphQueryBaseDAO.deductSharedBudgetAndCheckExhausted(budget, 5));
    assertTrue(budget.get() <= 0);
  }

  @Test
  public void testNegativeAddedTreatedAsZero() {
    AtomicInteger budget = new AtomicInteger(4);
    assertFalse(GraphQueryBaseDAO.deductSharedBudgetAndCheckExhausted(budget, -3));
    assertEquals(budget.get(), 4);
  }
}
