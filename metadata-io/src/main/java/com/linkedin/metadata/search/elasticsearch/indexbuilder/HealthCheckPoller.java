package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;

/**
 * Polls cluster health and updates CircuitBreakerState.
 *
 * <p>Executes a single health poll and maps the status to HealthState (GREEN/YELLOW/RED). Designed
 * to be scheduled at regular intervals by ScheduledExecutorService. The CircuitBreakerState handles
 * time-based hysteresis internally, preventing flapping between states.
 *
 * <p>Features: - Single-poll design (loop managed by ScheduledExecutorService) - Maps OpenSearch
 * ClusterHealthStatus to CircuitBreakerState.HealthState - Robust error handling that logs but
 * doesn't fail - Thread-safe CircuitBreakerState updates
 *
 * <p>Thread Safety: - Designed to be scheduled in a background thread pool -
 * CircuitBreakerState.update() is thread-safe for concurrent calls
 *
 * <p>Usage:
 *
 * <pre>
 * HealthCheckPoller poller = new HealthCheckPoller(indexBuilder, circuitBreakerState, 15, ...);
 * ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
 * executor.scheduleAtFixedRate(poller::poll, 0, 15, TimeUnit.SECONDS);
 * </pre>
 */
@Slf4j
public class HealthCheckPoller {

  private final ESIndexBuilder indexBuilder;
  private final CircuitBreakerState circuitBreakerState;
  private final double heapThresholdPercent;
  private final double heapYellowThresholdPercent;
  private final int writeRejectionRedThreshold;
  private final double yellowNodeFraction = 0.6; // 60% of nodes threshold
  private final Map<String, Long> previousRejectionsByNode = new ConcurrentHashMap<>();
  private final Map<String, Long> previousDeltaByNode = new ConcurrentHashMap<>();

  /**
   * Create a new health check poller.
   *
   * @param indexBuilder ESIndexBuilder for cluster health API access
   * @param circuitBreakerState CircuitBreakerState to update with health status
   * @param heapThresholdPercent Heap percentage threshold for RED (e.g., 90)
   * @param heapYellowThresholdPercent Heap percentage threshold for YELLOW (e.g., 75)
   * @param writeRejectionRedThreshold Delta threshold for immediate RED on 2 consecutive polls
   */
  public HealthCheckPoller(
      final ESIndexBuilder indexBuilder,
      final CircuitBreakerState circuitBreakerState,
      final double heapThresholdPercent,
      final double heapYellowThresholdPercent,
      final int writeRejectionRedThreshold) {
    this.indexBuilder = indexBuilder;
    this.circuitBreakerState = circuitBreakerState;
    this.heapThresholdPercent = heapThresholdPercent;
    this.heapYellowThresholdPercent = heapYellowThresholdPercent;
    this.writeRejectionRedThreshold = writeRejectionRedThreshold;
    log.info(
        "HealthCheckPoller started with heap threshold={}%, rejection threshold={}",
        heapThresholdPercent, writeRejectionRedThreshold);
  }

  /**
   * Execute a single health poll and update circuit breaker state.
   *
   * <p>Designed to be called repeatedly by ScheduledExecutorService. No blocking operations or
   * sleeps - the scheduler handles timing.
   */
  public void poll() {
    try {
      // Poll cluster health
      ClusterHealthResponse clusterHealth = indexBuilder.getClusterHealth();

      // Compute health signals from heap and rejection metrics
      HealthSignals signals = computeHealthSignals();

      // Determine tier based on cluster status and signals
      CircuitBreakerState.HealthState newState = determineTier(clusterHealth, signals);

      log.debug(
          "Cluster status: {}, signals: {}, tier: {}",
          clusterHealth.getStatus(),
          signals,
          newState);

      // Update circuit breaker (handles hysteresis internally)
      circuitBreakerState.update(newState);
    } catch (final IOException | OpenSearchException e) {
      log.warn(
          "Health check failed (network/API error): {}. Transitioning to RED.", e.getMessage());
      // Cluster unresponsive = RED state to block new reindex submissions
      circuitBreakerState.update(CircuitBreakerState.HealthState.RED);
    } catch (Exception e) {
      log.error("Unexpected exception in health check poll", e);
      // Assume cluster failure on unexpected errors
      circuitBreakerState.update(CircuitBreakerState.HealthState.RED);
    }
  }

  /**
   * Determine throttle tier based on cluster status and health signals.
   *
   * <p>Uses per-metric aggregation because each signal is already properly aggregated:
   *
   * <p><b>RED signals (any trigger immediately):</b> - Cluster health RED (ES aggregate judgment) -
   * Any single node heap >= 90% (critical, dangerous regardless of others) - Sustained rejection
   * spike (2 consecutive polls > threshold on ANY node)
   *
   * <p><b>YELLOW signals (majority-based, not per-node):</b> - Cluster health YELLOW WITH actual
   * impact (not just replica allocation issues) - >= 60% of nodes heap >= 75% (cluster-wide
   * elevation) - >= 60% of nodes with rejections this poll (widespread transient pressure)
   *
   * <p><b>Cluster YELLOW disambiguation:</b> Cluster YELLOW due to only unassigned replicas
   * (primary shards all assigned) is treated as GREEN because: - All primary data is accessible and
   * safe - Cluster performance is unaffected (no read/write impact) - Throttling would
   * unnecessarily slow reindex on a healthy cluster
   *
   * <p>Only cluster YELLOW with unassigned PRIMARY shards (data safety risk) triggers YELLOW tier.
   *
   * @param clusterHealth Full cluster health response (needed for shard analysis)
   * @param signals Per-metric aggregated health signals
   * @return HealthState for circuit breaker
   */
  private CircuitBreakerState.HealthState determineTier(
      final ClusterHealthResponse clusterHealth, final HealthSignals signals) {
    final ClusterHealthStatus status = clusterHealth.getStatus();

    log.debug(
        "determineTier: cluster.status={}, heapRed={}, heapYellow={}, rejectionRed={}, rejectionYellow={}",
        status,
        signals.heapRed(),
        signals.heapYellow(),
        signals.rejectionRed(),
        signals.rejectionYellow());

    // Immediate RED - bypass hysteresis
    if (status == ClusterHealthStatus.RED || signals.heapRed() || signals.rejectionRed()) {
      log.warn("Cluster health degraded to RED (heap or rejections critical)");
      return CircuitBreakerState.HealthState.RED;
    }

    // Candidate YELLOW - but check if cluster YELLOW is ONLY due to unassigned replicas
    // If replicas unassigned BUT heap/rejections are fine → treat as GREEN
    // If replicas unassigned AND heap/rejections are problematic → treat as YELLOW from signals
    if (status == ClusterHealthStatus.YELLOW) {
      boolean onlyReplicasUnassigned = isOnlyReplicasUnassigned(clusterHealth);
      log.debug(
          "Cluster YELLOW: onlyReplicasUnassigned={}, heapYellow={}, rejectionYellow={}",
          onlyReplicasUnassigned,
          signals.heapYellow(),
          signals.rejectionYellow());
      boolean heapAcceptable = signals.maxHeapPercent() < 85.0;
      boolean rejectionAcceptable = !signals.rejectionYellow();
      // When only replicas unassigned (no data safety risk), use a relaxed heap threshold
      // Normal threshold: 75%, Relaxed: 85%
      // This allows minor heap elevation (72%) to not block, while still protecting against
      // genuinely dangerous levels (88%+) that could trigger circuit breaker exceptions
      // Relaxed check: tolerate up to 85% heap when only replicas are unassigned
      if (onlyReplicasUnassigned && heapAcceptable && rejectionAcceptable) {
        log.debug("YELLOW only from replica assignment, treating as GREEN");
        return CircuitBreakerState.HealthState.GREEN;
      }
      // Cluster YELLOW with primary shard issues, or heap/rejections problematic → apply YELLOW
      return CircuitBreakerState.HealthState.YELLOW;
    }

    // Candidate YELLOW from heap or rejection metrics (even if cluster status is GREEN)
    if (signals.heapYellow() || signals.rejectionYellow()) {
      log.debug("Cluster health elevated to YELLOW (heap or rejections elevated)");
      return CircuitBreakerState.HealthState.YELLOW;
    }

    // All GREEN signals
    log.debug("Cluster health stable at GREEN");
    return CircuitBreakerState.HealthState.GREEN;
  }

  /**
   * Check if cluster YELLOW is only due to unassigned replica shards.
   *
   * <p>If all primary shards are assigned, the data is safe and accessible. Unassigned replicas are
   * a redundancy concern, not a performance concern (unless combined with heap/rejection issues).
   * The logic: active_primary_shards == indices*shards_per_index means all primaries are assigned.
   * If unassigned_shards > 0, they must be replicas.
   *
   * @param clusterHealth Cluster health response
   * @return true if unassigned shards are only replicas (no primary shards unassigned)
   */
  private boolean isOnlyReplicasUnassigned(final ClusterHealthResponse clusterHealth) {
    int unassignedShards = clusterHealth.getUnassignedShards();
    int relocatingShards = clusterHealth.getRelocatingShards();

    // In YELLOW: all primaries assigned, so initializing shards are only replicas recovering.
    // Only relocating indicates primary data movement (real issue).
    return unassignedShards > 0 && relocatingShards == 0;
  }

  /**
   * Compute health signals from cluster heap and rejection metrics.
   *
   * <p>Combines: - Heap pressure: any node >= 90% triggers RED, >= 60% of nodes >= 75% triggers
   * YELLOW - Thread pool rejections: 2 consecutive polls > threshold triggers RED, >= 60% nodes
   * rejecting triggers YELLOW
   *
   * @return HealthSignals aggregating all metric signals
   */
  private HealthSignals computeHealthSignals() {
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> heapMap;
    Map<String, OpenSearchJvmInfo.ThreadPoolRejections> rejMap;

    try {
      // Single consolidated call to get both heap and rejection metrics in one API call
      OpenSearchJvmInfo.NodeMetrics metrics = indexBuilder.getJvminfo().getDataNodeMetrics();
      heapMap = metrics.getHeapMap();
      rejMap = metrics.getRejectionMap();
    } catch (Exception e) {
      log.error("Failed to get node stats, defaulting to RED (unresponsive cluster)", e);
      // ES unresponsive = RED signal, don't throttle down further
      return new HealthSignals(true, false, false, false, 100.0); // heapRed=true = RED tier
    }

    int nodeCount = heapMap.size();
    if (nodeCount == 0) {
      log.warn("No data nodes found, defaulting to RED");
      return new HealthSignals(true, false, false, false, 100.0); // heapRed=true = RED tier
    }

    // --- Heap signals ---
    // Per-metric aggregation (not blanket OR):
    // - RED: Any single node in critical state (>= heapThresholdPercent, e.g., 90%) → immediate
    // - YELLOW: Majority of cluster elevated (>= 60% nodes >= heapYellowThresholdPercent, e.g.,
    // 75%)
    double maxHeapPercent =
        heapMap.values().stream()
            .mapToDouble(OpenSearchJvmInfo.JvmHeapInfo::getHeapUsedPercent)
            .max()
            .orElse(0.0);

    long heapRedCount =
        heapMap.values().stream()
            .filter(h -> h.getHeapUsedPercent() >= heapThresholdPercent)
            .count();
    long heapYellowCount =
        heapMap.values().stream()
            .filter(h -> h.getHeapUsedPercent() >= heapYellowThresholdPercent)
            .count();

    boolean heapRed = heapRedCount > 0; // ANY node critical
    boolean heapYellow = heapYellowCount >= nodeCount * yellowNodeFraction; // Majority elevated

    log.debug(
        "Heap: redCount={}, yellowCount={}, heapRed={}, heapYellow={}",
        heapRedCount,
        heapYellowCount,
        heapRed,
        heapYellow);

    // --- Rejection signals ---
    // Per-metric aggregation (not blanket OR):
    // - RED: Sustained spike (2 consecutive polls > threshold on ANY node) → cluster overload
    // - YELLOW: Majority nodes with transient rejections (>= 60%) → early warning, not sustained
    boolean rejRed = false;
    long rejYellowCount = 0;

    for (Map.Entry<String, OpenSearchJvmInfo.ThreadPoolRejections> entry : rejMap.entrySet()) {
      String node = entry.getKey();
      long currentTotal = entry.getValue().getTotalRejected();
      long prevTotal = previousRejectionsByNode.getOrDefault(node, currentTotal);
      long delta = currentTotal - prevTotal;
      long prevDelta = previousDeltaByNode.getOrDefault(node, 0L);

      log.debug(
          "Node {} rejections: delta={}, prevDelta={}, check={}",
          node,
          delta,
          prevDelta,
          (delta > writeRejectionRedThreshold && prevDelta > writeRejectionRedThreshold));

      // RED: Sustained rejection spike (2 consecutive polls both > threshold)
      // Indicates cluster cannot absorb write load consistently → immediate RED
      if (delta > writeRejectionRedThreshold && prevDelta > writeRejectionRedThreshold) {
        rejRed = true;
        log.warn("Sustained rejection spike detected on node {}", node);
      }
      // YELLOW: Count nodes with transient rejections this poll (any delta > 0)
      if (delta > 0) {
        rejYellowCount++;
      }

      // Update state for next poll
      previousRejectionsByNode.put(node, currentTotal);
      previousDeltaByNode.put(node, delta);
    }

    // YELLOW: Requires majority (>= 60%) of nodes with transient rejections
    // Prevents noise from single-node transient rejections
    boolean rejYellow = !rejMap.isEmpty() && rejYellowCount >= rejMap.size() * yellowNodeFraction;

    log.debug(
        "Rejections: yellowCount={}, rejYellow={}, rejRed={}", rejYellowCount, rejYellow, rejRed);

    return new HealthSignals(heapRed, heapYellow, rejRed, rejYellow, maxHeapPercent);
  }

  private static class HealthSignals {
    final boolean heapRed;
    final boolean heapYellow;
    final boolean rejectionRed;
    final boolean rejectionYellow;
    final double maxHeapPercent;

    HealthSignals(
        boolean heapRed,
        boolean heapYellow,
        boolean rejectionRed,
        boolean rejectionYellow,
        double maxHeapPercent) {
      this.heapRed = heapRed;
      this.heapYellow = heapYellow;
      this.rejectionRed = rejectionRed;
      this.rejectionYellow = rejectionYellow;
      this.maxHeapPercent = maxHeapPercent;
    }

    public boolean heapRed() {
      return heapRed;
    }

    public boolean heapYellow() {
      return heapYellow;
    }

    public boolean rejectionRed() {
      return rejectionRed;
    }

    public boolean rejectionYellow() {
      return rejectionYellow;
    }

    public double maxHeapPercent() {
      return maxHeapPercent;
    }

    @Override
    public String toString() {
      return String.format(
          "HealthSignals{heapRed=%s, heapYellow=%s, rejectionRed=%s, rejectionYellow=%s, maxHeapPercent=%.1f}",
          heapRed, heapYellow, rejectionRed, rejectionYellow, maxHeapPercent);
    }
  }
}
