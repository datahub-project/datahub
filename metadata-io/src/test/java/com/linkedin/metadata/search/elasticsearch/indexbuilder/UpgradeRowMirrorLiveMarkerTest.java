package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import com.linkedin.metadata.search.elasticsearch.update.MirroredWriteLog;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpgradeRowMirrorLiveMarkerTest {
  private static final String KEY =
      ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS);

  private DataHubUpgradeResultStore store;
  private OperationContext opContext;
  private Urn urn;
  private MirroredWriteLog writeLog;
  private UpgradeRowMirrorLiveMarker marker;

  @BeforeMethod
  public void setUp() throws Exception {
    store = mock(DataHubUpgradeResultStore.class);
    opContext = mock(OperationContext.class);
    urn = Urn.createFromString("urn:li:dataHubUpgrade:ClusterMigration_graph_primary_to_secondary");
    writeLog =
        new MirroredWriteLog(
            mock(ESBulkProcessor.class),
            MigrationIndexNames.forFamily(
                IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration()),
                SearchComponent.GRAPH,
                null));
    marker =
        new UpgradeRowMirrorLiveMarker(
            store, opContext, urn, Set.of(ClusterMigrationState.Process.GMS), writeLog);
  }

  private static EnvelopedAspect row(Map<String, String> result) {
    return DataHubUpgradeResultConditionalPersist.toEnveloped(
        new DataHubUpgradeResult()
            .setState(DataHubUpgradeState.IN_PROGRESS)
            .setResult(new StringMap(result)),
        "7");
  }

  private DataHubUpgradeResult written() throws Exception {
    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(store, times(1)).ingest(eq(opContext), captor.capture());
    return GenericRecordUtils.deserializeAspect(
        captor.getValue().getAspect().getValue(),
        captor.getValue().getAspect().getContentType(),
        DataHubUpgradeResult.class);
  }

  @Test
  public void testAPollRecordsTheMissingMarkerAndALaterPollFindsIt() throws Exception {
    Map<String, String> prepared = Map.of(ClusterMigrationState.STATUS, "PREPARED");
    // The deciding read, the conditional persist's own read, then the next poll's read.
    when(store.readLatest(opContext, urn))
        .thenReturn(
            row(prepared),
            row(prepared),
            row(Map.of(ClusterMigrationState.STATUS, "PREPARED", KEY, "1000000")));

    marker.poll();
    marker.poll();

    DataHubUpgradeResult written = written();
    assertFalse(written.getResult().get(KEY).isEmpty());
    assertEquals(written.getResult().get(ClusterMigrationState.STATUS), "PREPARED");
    assertTrue(marker.isRecorded());
    verify(store, times(3)).readLatest(opContext, urn);
  }

  @Test
  public void testAMarkerOfAnEarlierRolloutIsRecordedOver() throws Exception {
    // The row was stamped by a later system update than the pod that wrote "5"; this process
    // started after that stamp, so its start time is the marker that counts.
    when(store.readLatest(opContext, urn))
        .thenReturn(row(Map.of(KEY, "5", ClusterMigrationState.ROLLOUT_STARTED_AT, "100")));

    marker.poll();

    DataHubUpgradeResult written = written();
    assertTrue(Long.parseLong(written.getResult().get(KEY)) >= 100);
    assertTrue(marker.isRecorded());
  }

  @Test
  public void testAProcessStartedBeforeTheRolloutDoesNotClaimIt() throws Exception {
    // The row was stamped after this process started (the system-update job itself, or a pod up
    // since before the blocking job): writing its start would only be rewritten every minute.
    String later = String.valueOf(System.currentTimeMillis() + 60_000L);
    when(store.readLatest(opContext, urn))
        .thenReturn(row(Map.of(KEY, "5", ClusterMigrationState.ROLLOUT_STARTED_AT, later)));

    marker.poll();
    marker.poll();

    verify(store, never()).ingest(any(), any());
    assertFalse(marker.isRecorded());
  }

  @Test
  public void testAMarkerOfThisRolloutIsKept() throws Exception {
    // Another pod of this rollout recorded first; its earlier start is the moment that counts.
    String theirs = String.valueOf(System.currentTimeMillis());
    when(store.readLatest(opContext, urn))
        .thenReturn(row(Map.of(KEY, theirs, ClusterMigrationState.ROLLOUT_STARTED_AT, "100")));

    marker.poll();

    verify(store, never()).ingest(any(), any());
    assertTrue(marker.isRecorded());
  }

  @Test
  public void testAMarkerARowResetLostIsRecordedAgain() throws Exception {
    when(store.readLatest(opContext, urn))
        .thenReturn(row(Map.of(KEY, "5")), row(Map.of()), row(Map.of()));

    marker.poll();
    verify(store, never()).ingest(any(), any());
    marker.poll();

    verify(store, times(1)).ingest(any(), any());
    assertTrue(marker.isRecorded());
  }

  @Test
  public void testAMissingRowRecordsNothingAndKeepsTheLogOn() throws Exception {
    when(store.readLatest(opContext, urn)).thenReturn(null);
    writeLog.setActive(false);

    marker.poll();

    assertFalse(marker.isRecorded());
    verify(store, never()).ingest(any(), any());
    assertTrue(writeLog.isActive(), "a copy may be planned once the row exists");
  }

  @Test
  public void testTheDeleteLogFollowsTheCopyStatus() throws Exception {
    when(store.readLatest(opContext, urn))
        .thenReturn(
            row(Map.of(KEY, "5", ClusterMigrationState.STATUS, "VERIFIED")),
            row(Map.of(KEY, "5", ClusterMigrationState.STATUS, "COPYING")),
            row(
                Map.of(
                    KEY,
                    "5",
                    ClusterMigrationState.STATUS,
                    "PREPARED",
                    ClusterMigrationState.COPY_ENABLED,
                    "false")));

    marker.poll();
    assertFalse(writeLog.isActive(), "nothing replays the log once the copy has verified");

    marker.poll();
    assertTrue(writeLog.isActive(), "a copy planned again needs the log");

    marker.poll();
    assertFalse(writeLog.isActive(), "a pair the prepare step made mirror-only keeps no log");
  }

  @Test
  public void testAProcessWithTwoRolesRecordsBothKeysInOnePersist() throws Exception {
    String maeKey = ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE);
    UpgradeRowMirrorLiveMarker both =
        new UpgradeRowMirrorLiveMarker(
            store,
            opContext,
            urn,
            Set.of(ClusterMigrationState.Process.GMS, ClusterMigrationState.Process.MAE),
            null);
    when(store.readLatest(opContext, urn)).thenReturn(row(Map.of(KEY, "5")));

    both.poll();

    DataHubUpgradeResult written = written();
    // The role another process already recorded is left alone; the missing one is added.
    assertEquals(written.getResult().get(KEY), "5");
    assertFalse(written.getResult().get(maeKey).isEmpty());
    assertTrue(both.isRecorded());
  }

  @Test
  public void testPersistenceFailureIsLeftToTheNextPoll() throws Exception {
    when(store.readLatest(opContext, urn)).thenReturn(row(Map.of()));
    doThrow(new RuntimeException("boom")).when(store).ingest(any(), any());

    marker.poll();
    assertFalse(marker.isRecorded());
    marker.poll();

    // Two attempts, each a deciding read plus the conditional persist's own read.
    verify(store, times(4)).readLatest(opContext, urn);
    assertFalse(marker.isRecorded());
  }
}
