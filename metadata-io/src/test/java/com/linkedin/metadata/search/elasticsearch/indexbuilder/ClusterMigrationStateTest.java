package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import org.testng.annotations.Test;

public class ClusterMigrationStateTest {
  private static final String GMS =
      ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS);
  private static final String MAE =
      ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE);

  @Test
  public void testBothLiveWritersMustMarkTheCurrentRollout() {
    // No stamp: any marker counts.
    assertEquals(
        ClusterMigrationState.liveWritersMirroringSince(Map.of(GMS, "5", MAE, "7")).orElseThrow(),
        7L);
    assertTrue(ClusterMigrationState.liveWritersMirroringSince(Map.of(GMS, "5")).isEmpty());

    // A stamp later than a marker: that marker's pod belongs to an earlier rollout.
    Map<String, String> stamped =
        Map.of(GMS, "5", MAE, "200", ClusterMigrationState.ROLLOUT_STARTED_AT, "100");
    assertTrue(ClusterMigrationState.liveWritersMirroringSince(stamped).isEmpty());
    assertTrue(ClusterMigrationState.marksCurrentRollout(stamped, MAE));

    Map<String, String> bothNew =
        Map.of(GMS, "150", MAE, "200", ClusterMigrationState.ROLLOUT_STARTED_AT, "100");
    assertEquals(ClusterMigrationState.liveWritersMirroringSince(bothNew).orElseThrow(), 200L);
  }

  @Test
  public void testWritersOfThePreviousRolloutAreThoseStartedBeforeTheStampAndStillReporting() {
    long now = 1_000_000L;
    long fresh = now - 1_000L;
    long stale = now - ClusterMigrationState.WRITER_STALE_AFTER_MS - 1;
    Map<String, String> row =
        Map.of(
            // Started before the rollout and still reporting: the pod the copy must wait out.
            ClusterMigrationState.writerKey("gms-old"),
            ClusterMigrationState.writerValue(50, fresh),
            // Started before the rollout but gone: three missed reports.
            ClusterMigrationState.writerKey("mae-old"),
            ClusterMigrationState.writerValue(60, stale),
            // Started after the rollout: this rollout's pod, mirroring.
            ClusterMigrationState.writerKey("gms-new"),
            ClusterMigrationState.writerValue(150, fresh),
            // Not a report at all.
            ClusterMigrationState.STATUS,
            "PREPARED",
            ClusterMigrationState.writerKey("broken"),
            "not-a-report");

    assertEquals(
        ClusterMigrationState.previousRolloutWritersReporting(row, 100, now).stream()
            .map(ClusterMigrationState.Writer::instance)
            .toList(),
        java.util.List.of("gms-old"));
    assertTrue(ClusterMigrationState.previousRolloutWritersReporting(row, 40, now).isEmpty());
    assertEquals(ClusterMigrationState.writers(row).size(), 3);
  }
}
