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
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
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
    SystemAspect result = apply(lineage(10L), lineage(99L), comparator, "run-1", "run-2", 2_000L);
    assertEquals(result.getSystemMetadata().getVersion(), "2");
    assertFalse(result.isSemanticNoOp());
    assertEquals(comparator.walkCount(), 0);
    assertTrue(DataTemplateUtil.areEqual(result.getRecordTemplate(), lineage(99L)));
  }

  @Test
  public void ignoreKeepsStoredBodyVersionAndRunMetadata() throws Exception {
    SemanticNoOpComparator comparator = compile(IGNORE_STAMP);
    UpstreamLineage stored = lineage(10L);
    UpstreamLineage incoming = lineage(99L);
    SystemAspect result = apply(stored, incoming, comparator, "run-1", "run-2", 2_000L);

    assertEquals(result.getSystemMetadata().getVersion(), "1");
    assertEquals(result.getSystemMetadata().getRunId(), "run-2");
    assertEquals(result.getSystemMetadata().getLastRunId(), "run-1");
    assertEquals(result.getSystemMetadata().getLastObserved().longValue(), 2_000L);
    assertTrue(result.isSemanticNoOp());
    assertTrue(DataTemplateUtil.areEqual(result.getRecordTemplate(), stored));
    assertEquals(incoming.getUpstreams().get(0).getAuditStamp().getTime().longValue(), 99L);
    assertEquals(comparator.walkCount(), 1);

    SystemAspect changedGraph =
        apply(stored, lineage(10L, upstreamUrn("db.other")), comparator, "run-1", "run-3", 3_000L);
    assertEquals(changedGraph.getSystemMetadata().getVersion(), "2");
    assertFalse(changedGraph.isSemanticNoOp());
  }

  @Test
  public void timestampWindowCoalescesOnlyInsideTheThreshold() throws Exception {
    SemanticNoOpComparator comparator = compile(WINDOW_TIME);
    SystemAspect inside = apply(lineage(1_000L), lineage(2_000L), comparator, "run-1", "run-2", 2L);
    assertEquals(inside.getSystemMetadata().getVersion(), "1");
    assertTrue(DataTemplateUtil.areEqual(inside.getRecordTemplate(), lineage(1_000L)));

    SystemAspect outside =
        apply(lineage(1_000L), lineage(2_001L), comparator, "run-1", "run-2", 2L);
    assertEquals(outside.getSystemMetadata().getVersion(), "2");
    assertTrue(DataTemplateUtil.areEqual(outside.getRecordTemplate(), lineage(2_001L)));
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

  private SemanticNoOpComparator compile(String rules) {
    return SemanticNoOpComparator.compile(true, rules, registry, null);
  }

  private SystemAspect apply(
      UpstreamLineage stored,
      UpstreamLineage incoming,
      SemanticNoOpComparator comparator,
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
    return EntityServiceImpl.applyUpsert(change, latest, List.of(), null, opContext, comparator);
  }

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
