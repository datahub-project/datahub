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
}
