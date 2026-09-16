package com.linkedin.metadata.graph.elastic;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class GraphQueryTimeoutsTest {

  @Test
  public void testKeepsConfiguredWhenAlreadyLongerThanBudget() {
    // 120s configured comfortably exceeds 50s budget + 10s drain + 10s margin -> keep as
    // configured.
    assertEquals(GraphQueryTimeouts.computeEffectiveKeepAlive("120s", 50, 10), "120s");
  }

  @Test
  public void testRaisesToBudgetWhenConfiguredTooShort() {
    // 55s < 50 + 10 + 10 = 70 -> raised so the PIT outlives the query + drain.
    assertEquals(GraphQueryTimeouts.computeEffectiveKeepAlive("55s", 50, 10), "70s");
  }

  @Test
  public void testTreatsNullDrainAsZero() {
    // 50 + 0 + 10 = 60 -> "55s" is too short and is raised to 60s.
    assertEquals(GraphQueryTimeouts.computeEffectiveKeepAlive("55s", 50, 0), "60s");
  }
}
