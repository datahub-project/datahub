package com.linkedin.metadata.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.mxe.SystemMetadata;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** The per-aspect decision (layer 1): ceiling x locked latest -> delete whole / history / keep. */
public class CeilingDeletePlanTest {
  private static final String KEY = "corpUserKey";
  private static final long CAPTURED_AT = 5_000L;

  private static SystemAspect latest(long version) {
    final SystemAspect row = mock(SystemAspect.class);
    when(row.getSystemMetadataVersion()).thenReturn(Optional.of(version));
    return row;
  }

  private static SystemAspect latestCreatedAt(long version, long aspectCreatedMillis) {
    final SystemAspect row = latest(version);
    when(row.getSystemMetadata())
        .thenReturn(
            new SystemMetadata()
                .setVersion(String.valueOf(version))
                .setAspectCreated(
                    new AuditStamp()
                        .setTime(aspectCreatedMillis)
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"))));
    return row;
  }

  @DataProvider(name = "decisions")
  public Object[][] decisions() {
    // latest version, ceiling, deleted whole?, history deleted up to (null = none), survives?
    return new Object[][] {
      {3L, 3L, true, null, false}, // unchanged since the capture
      {2L, 3L, true, null, false}, // older than the capture (e.g. a rollback restored v2)
      {4L, 3L, false, 3L, true}, // written after the capture: latest kept, history 1..3 deleted
    };
  }

  @Test(dataProvider = "decisions")
  public void decidesEachAspectAgainstItsOwnCeiling(
      long latestVersion, long ceiling, boolean whole, Long historyUpTo, boolean survives) {
    final CeilingDeletePlan plan =
        CeilingDeletePlan.of(
            KEY, Map.of("status", latest(latestVersion)), Map.of("status", ceiling), CAPTURED_AT);

    assertEquals(plan.deleteWhole().contains("status"), whole);
    assertEquals(plan.deleteHistoryUpTo().get("status"), historyUpTo);
    assertEquals(plan.survivors().contains("status"), survives);
  }

  @Test
  public void aLegacyUnversionedLatestCountsAsVersionOne() {
    final SystemAspect legacy = mock(SystemAspect.class);
    when(legacy.getSystemMetadataVersion()).thenReturn(Optional.empty());
    when(legacy.getVersion()).thenReturn(0L);

    final CeilingDeletePlan plan =
        CeilingDeletePlan.of(KEY, Map.of("status", legacy), Map.of("status", 1L), CAPTURED_AT);

    assertEquals(plan.deleteWhole(), Set.of("status"));
    assertTrue(plan.survivors().isEmpty());
  }

  @Test
  public void nothingNewerMeansNoSurvivors() {
    final CeilingDeletePlan plan =
        CeilingDeletePlan.of(
            KEY,
            Map.of(KEY, latest(1L), "status", latest(2L), "globalTags", latest(5L)),
            Map.of("status", 2L, "globalTags", 5L),
            CAPTURED_AT);

    assertEquals(plan.deleteWhole(), Set.of("status", "globalTags"));
    assertTrue(plan.deleteHistoryUpTo().isEmpty());
    assertTrue(plan.survivors().isEmpty());
  }

  @Test
  public void aspectMissingFromTheCeilingSurvivesUntouched() {
    final CeilingDeletePlan plan =
        CeilingDeletePlan.of(
            KEY,
            Map.of("status", latest(1L), "globalTags", latest(1L)),
            Map.of("status", 1L),
            CAPTURED_AT);

    assertEquals(plan.deleteWhole(), Set.of("status"));
    assertEquals(plan.survivors(), Set.of("globalTags"));
    assertTrue(plan.deleteHistoryUpTo().isEmpty());
  }

  /**
   * Versions restart at 1 when an aspect is hard-deleted and written again, so a recreated aspect
   * can sit at or below its ceiling. Its creation time, after the capture, says it is newer.
   */
  @Test
  public void anAspectRecreatedAfterTheCaptureSurvivesUntouchedWhateverItsVersion() {
    final CeilingDeletePlan plan =
        CeilingDeletePlan.of(
            KEY,
            Map.of(
                "status",
                latestCreatedAt(1L, CAPTURED_AT + 1),
                "globalTags",
                latestCreatedAt(2L, CAPTURED_AT)),
            Map.of("status", 3L, "globalTags", 2L),
            CAPTURED_AT);

    assertEquals(plan.survivors(), Set.of("status"));
    assertEquals(plan.deleteWhole(), Set.of("globalTags"));
    assertTrue(plan.deleteHistoryUpTo().isEmpty());
  }

  @Test
  public void anAspectGoneSinceTheCaptureNeedsNothing() {
    final CeilingDeletePlan plan =
        CeilingDeletePlan.of(KEY, Map.of(), Map.of("status", 4L), CAPTURED_AT);

    assertTrue(plan.deleteWhole().isEmpty());
    assertTrue(plan.deleteHistoryUpTo().isEmpty());
    assertTrue(plan.survivors().isEmpty());
  }

  @Test
  public void theKeyIsNeverPlanned() {
    final CeilingDeletePlan plan =
        CeilingDeletePlan.of(KEY, Map.of(KEY, latest(1L)), Map.of(), CAPTURED_AT);

    assertTrue(plan.deleteWhole().isEmpty());
    assertTrue(plan.survivors().isEmpty());
  }
}
