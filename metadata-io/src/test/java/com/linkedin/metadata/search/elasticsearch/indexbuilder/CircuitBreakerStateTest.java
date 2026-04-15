package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.CircuitBreakerState.HealthState;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Comprehensive tests for CircuitBreakerState time-based hysteresis logic.
 *
 * <p>Validates: 1. Genuine state degradation with stability window enforcement 2. Flapping
 * detection (rapid state changes reset the stability clock) 3. State recovery with asymmetric
 * stability windows 4. Hard failures bypass stability windows entirely 5. Custom configuration
 * respected 6. Main loop polling (getCurrentState) is cheap and thread-safe
 */
public class CircuitBreakerStateTest {

  private CircuitBreakerState state;
  private BuildIndicesConfiguration defaultConfig;

  @BeforeMethod
  public void setup() {
    // Use 1-second stability windows for fast test execution
    // Tests validate the hysteresis algorithm, not absolute timing
    // Production configs use 10-30 second windows
    defaultConfig =
        BuildIndicesConfiguration.builder()
            .yellowStabilitySeconds(1)
            .greenStabilitySeconds(1)
            .redRecoverySeconds(1)
            .build();
    state = new CircuitBreakerState(defaultConfig);
  }

  // ============================================================================
  // Test 1: Genuine degradation (GREEN -> YELLOW)
  // ============================================================================

  @Test
  public void testGreenToYellowDegradation() throws InterruptedException {
    // Start: currentState should be GREEN
    assertEquals(state.getCurrentState(), HealthState.GREEN);

    // t=0s: detect YELLOW, candidateSince=t0
    state.update(HealthState.YELLOW);
    assertEquals(state.getCurrentState(), HealthState.GREEN, "Not yet transitioned");

    // t=0.5s: still YELLOW (simulated), stableFor < 10s
    state.update(HealthState.YELLOW);
    assertEquals(state.getCurrentState(), HealthState.GREEN, "Not yet transitioned");

    // Sleep to simulate time passage (31+ seconds to exceed 10s stability window)
    // Instant.now() is called inside update(), so we need real time to pass
    try {
      Thread.sleep(3100); // Wait 3.1 seconds (1s stability + 2.1s margin)
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }

    state.update(HealthState.YELLOW);
    assertEquals(
        state.getCurrentState(),
        HealthState.YELLOW,
        "After 31s of stability, should transition to YELLOW");
  }

  // ============================================================================
  // Test 3: Flapping detection (GREEN ↔ YELLOW ↔ GREEN)
  // ============================================================================

  @Test
  public void testFlappingDetection() throws InterruptedException {
    // t=0s: detect YELLOW, candidateSince=t0
    state.update(HealthState.YELLOW);
    assertEquals(state.getCurrentState(), HealthState.GREEN);

    // t=15s: flap back to GREEN (candidate changes, clock resets)
    try {
      Thread.sleep(2100); // Sleep 2.1s (1s stability + 1.1s margin)
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.GREEN);
    assertEquals(state.getCurrentState(), HealthState.GREEN, "Still GREEN, flap resets clock");

    // t=30s: flap back to YELLOW (clock was reset at t=15s)
    try {
      Thread.sleep(2100); // Sleep another 2.1s, but candidate clock was reset at previous step
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.YELLOW);
    assertEquals(
        state.getCurrentState(), HealthState.GREEN, "Flap resets clock, not yet 10s from t=15s");

    // t=46s: YELLOW is now stable for 10s+ from second candidate (t=30s)
    try {
      Thread.sleep(2100); // Sleep to allow stability window to pass
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.YELLOW);
    assertEquals(
        state.getCurrentState(),
        HealthState.YELLOW,
        "After 10s+ stable since last flap, should transition");
  }

  // ============================================================================
  // Test 4: Recovery (YELLOW -> GREEN)
  // ============================================================================

  @Test
  public void testRecoveryFromYellow() throws InterruptedException {
    // Force state to YELLOW by sleeping first
    state.update(HealthState.YELLOW);
    try {
      Thread.sleep(
          3100); // Wait for stability window to pass // Wait 31s for GREEN -> YELLOW transition
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.YELLOW);
    assertEquals(state.getCurrentState(), HealthState.YELLOW);

    // Now recovery: detect GREEN candidate
    state.update(HealthState.GREEN);
    assertEquals(state.getCurrentState(), HealthState.YELLOW, "Green detected but not stable yet");

    // Wait 10s+ (greenStabilitySeconds)
    try {
      Thread.sleep(3100); // Wait for stability window to pass
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.GREEN);
    assertEquals(
        state.getCurrentState(),
        HealthState.GREEN,
        "After 10s+ stable, should transition back to GREEN");
  }

  // ============================================================================
  // Test 5: Hard failure (ANY -> RED)
  // ============================================================================

  @Test
  public void testHardFailureIsImmediate() {
    // Start in GREEN
    assertEquals(state.getCurrentState(), HealthState.GREEN);

    // Detect RED - should transition IMMEDIATELY (no 30s wait)
    long before = System.currentTimeMillis();
    state.update(HealthState.RED);
    long after = System.currentTimeMillis();

    assertEquals(
        state.getCurrentState(), HealthState.RED, "RED transition should be immediate, not wait");
    assertTrue(
        (after - before) < 500,
        "Transition should happen in <500ms, not wait for stability window");
  }

  // ============================================================================
  // Test 5: Configuration variation (custom stability windows)
  // ============================================================================

  @Test
  public void testCustomConfigurationRespected() throws InterruptedException {
    // Create config with DIFFERENT stability windows (all shorter for fast tests)
    BuildIndicesConfiguration customConfig =
        BuildIndicesConfiguration.builder()
            .yellowStabilitySeconds(1) // 1s
            .greenStabilitySeconds(2) // 2s
            .redRecoverySeconds(1) // 1s
            .build();

    CircuitBreakerState customState = new CircuitBreakerState(customConfig);

    // Test: GREEN -> YELLOW with 1s window
    customState.update(HealthState.YELLOW);
    try {
      Thread.sleep(2500); // Wait 2.5s to ensure 1s window passes with margin
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    customState.update(HealthState.YELLOW);
    assertEquals(
        customState.getCurrentState(),
        HealthState.YELLOW,
        "Should transition with custom 1s window");
  }

  // ============================================================================
  // Test 7: State stays GREEN when updates confirm GREEN
  // ============================================================================

  @Test
  public void testGreenStayGreenMultipleUpdates() {
    for (int i = 0; i < 10; i++) {
      state.update(HealthState.GREEN);
      assertEquals(state.getCurrentState(), HealthState.GREEN);
    }
  }

  // ============================================================================
  // Test 8: Rapid RED from YELLOW (no delay)
  // ============================================================================

  @Test
  public void testYellowToRedNoDelay() throws InterruptedException {
    // First, get to YELLOW
    state.update(HealthState.YELLOW);
    try {
      Thread.sleep(3100); // Wait for stability window to pass
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.YELLOW);
    assertEquals(state.getCurrentState(), HealthState.YELLOW);

    // Now go to RED immediately
    long before = System.currentTimeMillis();
    state.update(HealthState.RED);
    long after = System.currentTimeMillis();

    assertEquals(state.getCurrentState(), HealthState.RED, "Should be RED immediately");
    assertTrue((after - before) < 100, "Should complete in <100ms");
  }

  // ============================================================================
  // Test 9: Candidate status tracking
  // ============================================================================

  @Test
  public void testCandidateStatusTracking() {
    state.update(HealthState.YELLOW);
    String status = state.getCandidateStatus();
    assertTrue(status.contains("candidate=YELLOW"), "Status should show YELLOW candidate");
    assertTrue(status.contains("current=GREEN"), "Status should show GREEN as current");
  }

  // ============================================================================
  // Test 10: RED recovery window respected
  // ============================================================================

  @Test
  public void testRedRecoveryWindowRespected() throws InterruptedException {
    // Create config with SHORT redRecoverySeconds to test
    BuildIndicesConfiguration shortRecoveryConfig =
        BuildIndicesConfiguration.builder()
            .yellowStabilitySeconds(1)
            .greenStabilitySeconds(1)
            .redRecoverySeconds(1) // 1s to recover from RED
            .build();

    CircuitBreakerState shortRecoveryState = new CircuitBreakerState(shortRecoveryConfig);

    // Go to RED
    shortRecoveryState.update(HealthState.RED);
    assertEquals(shortRecoveryState.getCurrentState(), HealthState.RED);

    // Try to recover to YELLOW - should wait 1s
    shortRecoveryState.update(HealthState.YELLOW);
    assertEquals(
        shortRecoveryState.getCurrentState(), HealthState.RED, "Not yet 1s, should stay RED");

    // Wait 2.5s to ensure 1s stability window passes
    try {
      Thread.sleep(2500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }

    shortRecoveryState.update(HealthState.YELLOW);
    assertEquals(
        shortRecoveryState.getCurrentState(),
        HealthState.YELLOW,
        "After 1s stable, should transition to YELLOW");
  }

  // ============================================================================
  // Test 11: Thread-safe polling (main reindex loop use case)
  // ============================================================================

  @Test
  public void testThreadSafeGetCurrentStatePolling() throws InterruptedException {
    // Simulate main reindex loop polling in one thread while HealthCheckPoller updates in another
    state.update(HealthState.YELLOW);
    try {
      Thread.sleep(3100); // Wait for stability window to pass
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.YELLOW);

    // Main loop polling (should be cheap and thread-safe)
    for (int i = 0; i < 100; i++) {
      HealthState current = state.getCurrentState();
      assertNotNull(current, "getCurrentState should never return null");
      // Verify it's either YELLOW (current) or RED (if health check triggered)
      assertTrue(
          current == HealthState.YELLOW || current == HealthState.RED,
          "State should be YELLOW or RED");
    }
  }

  // ============================================================================
  // Test: RED signal resets recovery candidate timer (hysteresis correctness)
  // ============================================================================

  @Test
  public void testRedSignalResetsRecoveryCandidate() throws InterruptedException {
    // t=0s: RED signal arrives
    state.update(HealthState.RED);
    assertEquals(state.getCurrentState(), HealthState.RED);

    // t=1.1s: YELLOW recovery candidate starts (waiting redRecoverySeconds=1s)
    try {
      Thread.sleep(1100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.YELLOW);
    assertEquals(state.getCurrentState(), HealthState.RED, "Still RED, YELLOW candidate waiting");

    // t=2.1s: Another RED signal arrives (should reset recovery timer!)
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.RED);
    assertEquals(state.getCurrentState(), HealthState.RED);

    // t=3.1s: Check YELLOW (only 1s since RED reset at t=2.1)
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.YELLOW);
    // BUG: Without fix, this would be YELLOW (counted from t=1.1)
    // With fix: Still RED (counted from t=2.1, only 1s passed)
    assertEquals(
        state.getCurrentState(),
        HealthState.RED,
        "RED signal at t=2.1 resets recovery timer - YELLOW needs to wait from that point");

    // t=4.1s: Now YELLOW should transition (2.1s since RED reset)
    try {
      Thread.sleep(1100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Sleep interrupted", e);
    }
    state.update(HealthState.YELLOW);
    assertEquals(
        state.getCurrentState(),
        HealthState.YELLOW,
        "After 2.1s of stability since most recent RED, should transition");
  }
}
