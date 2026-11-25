package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.EbeanSystemAspect;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.semantic.SemanticNoOpComparator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Timestamp;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SemanticNoOpEntityServiceTest {
  private static final String IGNORE_STAMP =
      "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp\",\"strategy\":\"IGNORE\"}]";
  private static final String WINDOW_TIME =
      "[{\"aspect\":\"upstreamLineage\",\"path\":\"/upstreams/*/auditStamp/time\","
          + "\"strategy\":\"TIMESTAMP_WINDOW\",\"maxDeltaMs\":1000}]";

  private OperationContext opContext;
  private EntityRegistry registry;
  private AuditStamp auditStamp;
  private Urn datasetUrn;
  private Urn upstreamUrn;

  @BeforeClass
  public void setup() throws Exception {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    registry = opContext.getEntityRegistry();
    auditStamp = new AuditStamp().setTime(1L).setActor(new CorpuserUrn("tester"));
    datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.downstream,PROD)");
    upstreamUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.upstream,PROD)");
  }

  @Test
  public void timestampOnlyChangeVersionsWhenComparisonIsOff() throws Exception {
    SemanticNoOpComparator comparator = SemanticNoOpComparator.disabled();
    AppliedWrite result = apply(lineage(10L), lineage(99L), comparator, "run-1", "run-2", 2_000L);
    assertEquals(result.aspect().getSystemMetadata().getVersion(), "2");
    assertFalse(result.change().isSemanticNoOp());
    assertEquals(comparator.walkCount(), 0);
    assertTrue(DataTemplateUtil.areEqual(result.aspect().getRecordTemplate(), lineage(99L)));
  }

  @Test
  public void ignoreKeepsStoredBodyVersionAndRunMetadata() throws Exception {
    SemanticNoOpComparator comparator = compile(IGNORE_STAMP);
    UpstreamLineage stored = lineage(10L);
    UpstreamLineage incoming = lineage(99L);
    AppliedWrite result = apply(stored, incoming, comparator, "run-1", "run-2", 2_000L);

    assertEquals(result.aspect().getSystemMetadata().getVersion(), "1");
    assertEquals(result.aspect().getSystemMetadata().getRunId(), "run-2");
    assertEquals(result.aspect().getSystemMetadata().getLastRunId(), "run-1");
    assertEquals(result.aspect().getSystemMetadata().getLastObserved().longValue(), 2_000L);
    assertTrue(result.change().isSemanticNoOp());
    assertTrue(DataTemplateUtil.areEqual(result.aspect().getRecordTemplate(), stored));
    assertEquals(incoming.getUpstreams().get(0).getAuditStamp().getTime().longValue(), 99L);
    assertEquals(comparator.walkCount(), 1);

    AppliedWrite changedGraph =
        apply(stored, lineage(10L, upstreamUrn("db.other")), comparator, "run-1", "run-3", 3_000L);
    assertEquals(changedGraph.aspect().getSystemMetadata().getVersion(), "2");
    assertFalse(changedGraph.change().isSemanticNoOp());
  }

  @Test
  public void timestampWindowCoalescesOnlyInsideTheThreshold() throws Exception {
    SemanticNoOpComparator comparator = compile(WINDOW_TIME);
    AppliedWrite inside = apply(lineage(1_000L), lineage(2_000L), comparator, "run-1", "run-2", 2L);
    assertEquals(inside.aspect().getSystemMetadata().getVersion(), "1");
    assertTrue(DataTemplateUtil.areEqual(inside.aspect().getRecordTemplate(), lineage(1_000L)));

    AppliedWrite outside =
        apply(lineage(1_000L), lineage(2_001L), comparator, "run-1", "run-2", 2L);
    assertEquals(outside.aspect().getSystemMetadata().getVersion(), "2");
    assertTrue(DataTemplateUtil.areEqual(outside.aspect().getRecordTemplate(), lineage(2_001L)));
  }

  @Test
  public void persistTimeRematchDoesNotMarkARealUpdateAsSemanticNoOp() throws Exception {
    SemanticNoOpComparator comparator = compile(IGNORE_STAMP);
    WriteAttempt write =
        write(lineage(10L), lineage(10L, upstreamUrn("db.other")), "run-1", "run-2", 2_000L);

    EntityServiceImpl.applyUpsert(
        write.change, write.latest, List.of(), null, opContext, comparator);
    assertFalse(write.change.isSemanticNoOp());
    assertEquals(comparator.walkCount(), 1);

    // Production rematches the same mutable aspect while persisting. Its body now strictly equals
    // the MCP body, so this must stay a real version bump and must not increment suppression again.
    EntityServiceImpl.applyUpsert(
        write.change, write.latest, List.of(), null, opContext, comparator);
    assertFalse(write.change.isSemanticNoOp());
    assertEquals(comparator.walkCount(), 1);

    EntityServiceImpl service =
        new EntityServiceImpl(
            mock(AspectDao.class),
            mock(com.linkedin.metadata.event.EventProducer.class),
            mock(PreProcessHooks.class),
            new EntityServiceConfiguration().setPostCommitRetentionEnabled(true),
            null);
    @SuppressWarnings("unchecked")
    RetentionService<ChangeItemImpl> retention = mock(RetentionService.class);
    service.setRetentionService(retention);
    service.applyRetentionPostCommit(
        opContext,
        List.of(
            upsertResult(
                lineage(10L),
                lineage(10L, upstreamUrn("db.other")),
                write.change.isSemanticNoOp(),
                3L)));
    verify(retention).applyRetentionWithPolicyDefaults(any(), anyList());
  }

  @Test
  public void semanticNoOpDecisionSurvivesThePersistTimeRematch() throws Exception {
    SemanticNoOpComparator comparator = compile(IGNORE_STAMP);
    WriteAttempt write = write(lineage(10L), lineage(99L), "run-1", "run-2", 2_000L);

    EntityServiceImpl.applyUpsert(
        write.change, write.latest, List.of(), null, opContext, comparator);

    assertTrue(write.change.isSemanticNoOp());
    EntityServiceImpl.applyUpsert(
        write.change, write.latest, List.of(), null, opContext, comparator);
    assertTrue(write.change.isSemanticNoOp());
    assertEquals(comparator.walkCount(), 1);
  }

  @Test
  public void mutatedProposalAfterSemanticNoOpIsComparedAgain() throws Exception {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricUtils metrics = MetricUtils.builder().registry(meterRegistry).build();
    SemanticNoOpComparator comparator =
        SemanticNoOpComparator.compile(true, IGNORE_STAMP, registry, metrics);
    WriteAttempt write = write(lineage(10L), lineage(99L), "run-1", "run-2", 2_000L);

    EntityServiceImpl.applyUpsert(
        write.change, write.latest, List.of(), null, opContext, comparator);
    assertTrue(write.change.isSemanticNoOp());
    assertEquals(suppressedCount(meterRegistry, "IGNORE"), 1d);

    ((UpstreamLineage) write.change.getRecordTemplate())
        .getUpstreams()
        .get(0)
        .setDataset(DatasetUrn.createFromUrn(upstreamUrn("db.other")));
    write.change.invalidateSemanticNoOp();

    EntityServiceImpl.applyUpsert(
        write.change, write.latest, List.of(), null, opContext, comparator);
    assertFalse(write.change.isSemanticNoOp());
    assertEquals(write.latest.getSystemMetadata().getVersion(), "2");
    assertEquals(comparator.walkCount(), 2);
    assertEquals(suppressedCount(meterRegistry, "IGNORE"), 0d);
    assertTrue(
        DataTemplateUtil.areEqual(
            write.latest.getRecordTemplate(), write.change.getRecordTemplate()));
  }

  private static double suppressedCount(SimpleMeterRegistry meterRegistry, String strategy) {
    return meterRegistry
        .get(SemanticNoOpComparator.SUPPRESSED_COUNTER)
        .tag("aspect", "upstreamLineage")
        .tag("strategy", strategy)
        .counter()
        .count();
  }

  @Test
  public void semanticNoOpDoesNotEnqueueRetentionButAVersionBumpDoes() throws Exception {
    EntityServiceImpl service =
        new EntityServiceImpl(
            mock(AspectDao.class),
            mock(com.linkedin.metadata.event.EventProducer.class),
            mock(PreProcessHooks.class),
            new EntityServiceConfiguration()
                .setPostCommitRetentionEnabled(true)
                .setEnableBrowseV2(true),
            null);
    @SuppressWarnings("unchecked")
    RetentionService<com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl> retention =
        mock(RetentionService.class);
    service.setRetentionService(retention);

    UpdateAspectResult suppressed = upsertResult(lineage(1L), lineage(2L), true, 0L);
    UpdateAspectResult bumped = upsertResult(lineage(1L), lineage(2L), false, 3L);

    service.applyRetentionPostCommit(opContext, List.of(suppressed));
    verify(retention, never()).applyRetentionWithPolicyDefaults(any(), anyList());

    service.applyRetentionPostCommit(opContext, List.of(bumped));
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<RetentionService.RetentionContext>> captured =
        ArgumentCaptor.forClass(List.class);
    verify(retention, times(1)).applyRetentionWithPolicyDefaults(any(), captured.capture());
    assertEquals(captured.getValue().size(), 1);
    assertEquals(captured.getValue().get(0).getAspectName(), "upstreamLineage");
  }

  @Test
  public void unchangedBodyDoesNotEnqueueRetentionWithoutARule() throws Exception {
    EntityServiceImpl service =
        new EntityServiceImpl(
            mock(AspectDao.class),
            mock(com.linkedin.metadata.event.EventProducer.class),
            mock(PreProcessHooks.class),
            new EntityServiceConfiguration().setPostCommitRetentionEnabled(true),
            null);
    @SuppressWarnings("unchecked")
    RetentionService<ChangeItemImpl> retention = mock(RetentionService.class);
    service.setRetentionService(retention);

    // A re-sent aspect is a separate record with an equal body and is not a semantic no-op. It
    // keeps the stored version, so no history row was written (maxVersion 0).
    UpstreamLineage stored = lineage(1L);
    UpstreamLineage resent = lineage(1L);
    assertTrue(stored != resent);
    service.applyRetentionPostCommit(opContext, List.of(upsertResult(stored, resent, false, 0L)));
    verify(retention, never()).applyRetentionWithPolicyDefaults(any(), anyList());

    service.applyRetentionPostCommit(
        opContext, List.of(upsertResult(stored, lineage(2L), false, 3L)));
    verify(retention, times(1)).applyRetentionWithPolicyDefaults(any(), anyList());
  }

  @Test
  public void equalBodyThatWroteAHistoryRowIsStillTrimmed() throws Exception {
    EntityServiceImpl service =
        new EntityServiceImpl(
            mock(AspectDao.class),
            mock(com.linkedin.metadata.event.EventProducer.class),
            mock(PreProcessHooks.class),
            new EntityServiceConfiguration().setPostCommitRetentionEnabled(true),
            null);
    @SuppressWarnings("unchecked")
    RetentionService<ChangeItemImpl> retention = mock(RetentionService.class);
    service.setRetentionService(retention);

    // A write hook can make the final body equal after the version advanced and a history row was
    // written. Skipping retention here would leave that row untrimmed.
    service.applyRetentionPostCommit(
        opContext, List.of(upsertResult(lineage(1L), lineage(1L), false, 4L)));
    verify(retention, times(1)).applyRetentionWithPolicyDefaults(any(), anyList());
  }

  @Test
  public void bodyChangeWithoutAHistoryRowIsStillTrimmed() throws Exception {
    EntityServiceImpl service =
        new EntityServiceImpl(
            mock(AspectDao.class),
            mock(com.linkedin.metadata.event.EventProducer.class),
            mock(PreProcessHooks.class),
            new EntityServiceConfiguration().setPostCommitRetentionEnabled(true),
            null);
    @SuppressWarnings("unchecked")
    RetentionService<ChangeItemImpl> retention = mock(RetentionService.class);
    service.setRetentionService(retention);

    // A time-only policy keeps one version, so a real change writes no history row (maxVersion 0).
    // Retention still runs to age out older rows.
    service.applyRetentionPostCommit(
        opContext, List.of(upsertResult(lineage(1L), lineage(2L), false, 0L)));
    verify(retention, times(1)).applyRetentionWithPolicyDefaults(any(), anyList());
  }

  private SemanticNoOpComparator compile(String rules) {
    return SemanticNoOpComparator.compile(true, rules, registry, null);
  }

  private AppliedWrite apply(
      UpstreamLineage stored,
      UpstreamLineage incoming,
      SemanticNoOpComparator comparator,
      String previousRun,
      String nextRun,
      long observed)
      throws Exception {
    WriteAttempt write = write(stored, incoming, previousRun, nextRun, observed);
    SystemAspect aspect =
        EntityServiceImpl.applyUpsert(
            write.change, write.latest, List.of(), null, opContext, comparator);
    return new AppliedWrite(aspect, write.change);
  }

  private WriteAttempt write(
      UpstreamLineage stored,
      UpstreamLineage incoming,
      String previousRun,
      String nextRun,
      long observed)
      throws Exception {
    SystemMetadata initial = new SystemMetadata();
    initial.setRunId(previousRun);
    initial.setVersion("1");
    initial.setLastObserved(1L);
    EbeanAspectV2 row =
        new EbeanAspectV2(
            datasetUrn.toString(),
            "upstreamLineage",
            0L,
            RecordUtils.toJsonString(stored),
            new Timestamp(auditStamp.getTime()),
            auditStamp.getActor().toString(),
            null,
            RecordUtils.toJsonString(initial));
    SystemAspect latest = EbeanSystemAspect.builder().forUpdate(row, registry);
    SystemMetadata incomingMetadata = new SystemMetadata();
    incomingMetadata.setRunId(nextRun);
    incomingMetadata.setLastObserved(observed);
    ChangeMCP change =
        ChangeItemImpl.builder()
            .urn(datasetUrn)
            .aspectName("upstreamLineage")
            .recordTemplate(incoming)
            .systemMetadata(incomingMetadata)
            .auditStamp(auditStamp)
            .nextAspectVersion(2L)
            .build(opContext.getAspectRetriever());
    return new WriteAttempt(latest, change);
  }

  private record WriteAttempt(SystemAspect latest, ChangeMCP change) {}

  private record AppliedWrite(SystemAspect aspect, ChangeMCP change) {}

  private UpdateAspectResult upsertResult(
      UpstreamLineage oldValue, UpstreamLineage newValue, boolean semanticNoOp, long maxVersion) {
    ChangeMCP request = mock(ChangeMCP.class);
    org.mockito.Mockito.when(request.getAspectName()).thenReturn("upstreamLineage");
    return UpdateAspectResult.builder()
        .urn(datasetUrn)
        .request(request)
        .oldValue(oldValue)
        .newValue(newValue)
        .semanticNoOp(semanticNoOp)
        .maxVersion(maxVersion)
        .newSystemMetadata(new SystemMetadata())
        .auditStamp(auditStamp)
        .build();
  }

  private UpstreamLineage lineage(long time) throws Exception {
    return lineage(time, upstreamUrn);
  }

  private Urn upstreamUrn(String name) {
    return UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)");
  }

  private UpstreamLineage lineage(long time, Urn upstream) throws Exception {
    return new UpstreamLineage()
        .setUpstreams(
            new UpstreamArray(
                new Upstream()
                    .setDataset(DatasetUrn.createFromUrn(upstream))
                    .setType(DatasetLineageType.TRANSFORMED)
                    .setAuditStamp(
                        new AuditStamp().setTime(time).setActor(new CorpuserUrn("unknown")))));
  }
}
