package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Time-based circuit breaker state machine with hysteresis (stability windows).
 *
 * <p>Prevents flapping between health states by requiring candidate states to be stable for a
 * configured duration before transitioning. RED transitions bypass the stability window for
 * immediate failure detection.
 *
 * <p>Thread-safe design: - Uses AtomicReference for currentState reads - Uses synchronized for
 * update operations (infrequent)
 *
 * <p>Usage:
 *
 * <pre>
 * CircuitBreakerState state = new CircuitBreakerState(config);
 *
 * // HealthCheckPoller thread calls update() continuously
 * state.update(HealthState.YELLOW);
 * // If YELLOW is stable for yellowStabilitySeconds, transitions to YELLOW
 *
 * state.update(HealthState.RED);
 * // Transitions to RED immediately (no stability window)
 *
 * // Main reindex loop polls state as needed (cheap operation)
 * CircuitBreakerState.HealthState current = state.getCurrentState();
 * </pre>
 */
@Slf4j
public class CircuitBreakerState {

  public enum HealthState {
    GREEN {
      @Override
      public String getRefreshInterval(BuildIndicesConfiguration config) {
        return "-1";
      }

      @Override
      public float getRequestsPerSecond(BuildIndicesConfiguration config) {
        return -1.0f; // Unlimited
      }
    },
    YELLOW {
      @Override
      public String getRefreshInterval(BuildIndicesConfiguration config) {
        return config.getNormalTierRefreshInterval();
      }

      @Override
      public float getRequestsPerSecond(BuildIndicesConfiguration config) {
        return config.getNormalTierRequestsPerSecond();
      }
    },
    RED {
      @Override
      public String getRefreshInterval(BuildIndicesConfiguration config) {
        return config.getThrottledTierRefreshInterval();
      }

      @Override
      public float getRequestsPerSecond(BuildIndicesConfiguration config) {
        return config.getThrottledTierRequestsPerSecond();
      }
    };

    /**
     * Get health-aware refresh interval for this state.
     *
     * @return Refresh interval string for Elasticsearch
     */
    public abstract String getRefreshInterval(BuildIndicesConfiguration config);

    /**
     * Get health-aware request rate limit for this state.
     *
     * @return Requests per second, or -1 for unlimited
     */
    public abstract float getRequestsPerSecond(BuildIndicesConfiguration config);
  }

  private final BuildIndicesConfiguration config;
  private final AtomicReference<HealthState> currentState;
  private volatile HealthState candidateState;
  private volatile Instant candidateSince;

  /**
   * Create a new circuit breaker state with GREEN as initial state.
   *
   * @param config BuildIndicesConfiguration with stability window settings
   */
  public CircuitBreakerState(final BuildIndicesConfiguration config) {
    this.config = config;
    this.currentState = new AtomicReference<>(HealthState.GREEN);
    this.candidateState = HealthState.GREEN;
    this.candidateSince = Instant.now();
    log.info(
        "CircuitBreakerState initialized with stability windows: "
            + "yellowStability={}s, greenStability={}s, redRecovery={}s",
        config.getYellowStabilitySeconds(),
        config.getGreenStabilitySeconds(),
        config.getRedRecoverySeconds());
  }

  /**
   * Update the health state. Handles transitions with time-based hysteresis.
   *
   * <p>Rules: - RED transitions are immediate (no stability window) - Other transitions require
   * candidate state to be stable for getStabilityWindow() duration - State changes on flapping
   * (different candidate) reset the stability clock
   *
   * <p>Thread-safe: Can be called from multiple threads (HealthCheckPoller, test threads, etc.)
   *
   * @param newState New health state to process
   */
  public synchronized void update(final HealthState newState) {
    final HealthState prevState = currentState.get();

    log.debug(
        "CircuitBreakerState.update: newState={}, currentState={}, candidateState={}",
        newState,
        prevState,
        candidateState);

    // RED transitions are always immediate - no stability window
    if (newState == HealthState.RED) {
      if (prevState != HealthState.RED) {
        log.info("Immediate RED transition: {} -> RED (failure detected)", prevState);
        transition(HealthState.RED);
      } else {
        // Reset recovery candidate even if staying RED
        // Ensures each RED signal restarts the recovery timer for correct hysteresis
        candidateState = HealthState.RED;
        candidateSince = Instant.now();
      }
      return;
    }

    // Check if candidate state is being reinforced
    if (newState == candidateState) {
      final Duration stableFor = Duration.between(candidateSince, Instant.now());
      final Duration required = getStabilityWindow(prevState, candidateState);

      if (stableFor.compareTo(required) >= 0) {
        // Candidate has been stable long enough - transition
        log.info(
            "Stable transition: {} -> {} (stable for {}s >= required {}s)",
            prevState,
            candidateState,
            stableFor.getSeconds(),
            required.getSeconds());
        transition(candidateState);
      } else {
        // Still waiting for stability
        log.debug(
            "Awaiting stability for {} -> {}: {}s < {}s required",
            prevState,
            candidateState,
            stableFor.getSeconds(),
            required.getSeconds());
      }
    } else {
      // New candidate state - reset the clock (flap detection)
      log.debug(
          "New candidate detected: {} -> {} (previous candidate was {}), resetting clock",
          prevState,
          newState,
          candidateState);
      candidateState = newState;
      candidateSince = Instant.now();
    }
  }

  /**
   * Get the required stability window duration for a state transition.
   *
   * <p>Stability windows are asymmetric - recovery from RED takes longer than detection of RED.
   *
   * @param currentState The current state
   * @param candidateState The candidate state being evaluated
   * @return Duration that candidateState must remain stable before transitioning
   */
  private Duration getStabilityWindow(
      final HealthState currentState, final HealthState candidateState) {
    // GREEN -> YELLOW: requires yellowStabilitySeconds
    if (currentState == HealthState.GREEN && candidateState == HealthState.YELLOW) {
      return Duration.ofSeconds(config.getYellowStabilitySeconds());
    }

    // RED -> YELLOW: requires redRecoverySeconds (longer - we want to be sure)
    if (currentState == HealthState.RED && candidateState == HealthState.YELLOW) {
      return Duration.ofSeconds(config.getRedRecoverySeconds());
    }

    // YELLOW -> GREEN: requires greenStabilitySeconds
    if (currentState == HealthState.YELLOW && candidateState == HealthState.GREEN) {
      return Duration.ofSeconds(config.getGreenStabilitySeconds());
    }

    // RED -> GREEN: requires redRecoverySeconds (protect against brief metric dips during RED
    // episodes)
    // If cluster metrics dip below GREEN threshold during RED, avoid immediately opening circuit
    // and submitting new tasks at full speed - could trigger another RED spike
    if (currentState == HealthState.RED && candidateState == HealthState.GREEN) {
      return Duration.ofSeconds(config.getRedRecoverySeconds());
    }

    // GREEN -> RED is handled in update() with immediate transition
    // YELLOW -> RED is handled in update() with immediate transition
    // Same state: no transition needed (duration is 0)
    return Duration.ZERO;
  }

  /**
   * Execute a state transition.
   *
   * @param newState The new state to transition to
   */
  private void transition(final HealthState newState) {
    final HealthState prevState = currentState.getAndSet(newState);
    if (prevState != newState) {
      // Reset candidate to new current state
      candidateState = newState;
      candidateSince = Instant.now();
    }
  }

  /**
   * Get the current health state.
   *
   * <p>Thread-safe read operation.
   *
   * @return Current health state
   */
  public HealthState getCurrentState() {
    return currentState.get();
  }

  /**
   * Get the current candidate state and how long it's been stable.
   *
   * <p>For debugging and monitoring only.
   *
   * @return String representation of candidate state and stability duration
   */
  public synchronized String getCandidateStatus() {
    final Duration stableFor = Duration.between(candidateSince, Instant.now());
    return String.format(
        "candidate=%s, stableFor=%ds, current=%s",
        candidateState, stableFor.getSeconds(), currentState.get());
  }
}
