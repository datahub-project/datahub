package com.linkedin.datahub.upgrade.system.restoreindices;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestoreIndicesStreamUtilTest {

  @Mock private OperationContext mockOpContext;
  @Mock private EntityService<?> mockEntityService;
  @Mock private AspectDao mockAspectDao;
  @Mock private RetrieverContext mockRetrieverContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
  }

  /**
   * The scan must target the requested aspect and urn prefix (this is what makes each restore-index
   * step reindex the correct entities); an empty result set produces no MCLs.
   */
  @Test
  public void testScansRequestedAspectAndUrnPrefix() {
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(mockStream);
    when(mockStream.partition(anyInt())).thenReturn(Stream.empty());

    RestoreIndicesStreamUtil.reindexAspect(
        mockOpContext,
        mockEntityService,
        mockAspectDao,
        "upstreamLineage",
        "urn:li:dataset:%",
        1000,
        0,
        0,
        ChangeType.RESTATE);

    ArgumentCaptor<RestoreIndicesArgs> captor = ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao).streamAspectBatches(any(OperationContext.class), captor.capture());
    assertEquals(captor.getValue().aspectName(), "upstreamLineage");
    assertEquals(captor.getValue().urnLike(), "urn:li:dataset:%");
    assertEquals(captor.getValue().batchSize(), 1000);

    // No aspects streamed -> no MCLs emitted.
    verifyNoInteractions(mockEntityService);
  }

  /**
   * Non-empty batch: an MCL is produced per aspect with the requested change type, the run id is
   * stamped onto system metadata when provided, and the progress callback fires with the last urn.
   * Covers the shared emission loop that every restore-index step depends on.
   */
  @Test
  public void testEmitsMclStampsRunIdAndFiresProgressCallback() {
    EbeanAspectV2 row = mock(EbeanAspectV2.class);
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(mockStream);
    when(mockStream.partition(anyInt())).thenReturn(Stream.of(Stream.of(row)));

    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.t,PROD)");
    SystemMetadata systemMetadata = new SystemMetadata();
    SystemAspect systemAspect = mock(SystemAspect.class);
    when(systemAspect.getUrn()).thenReturn(urn);
    when(systemAspect.getAspectSpec()).thenReturn(mock(AspectSpec.class));
    when(systemAspect.getRecordTemplate()).thenReturn(mock(RecordTemplate.class));
    when(systemAspect.getSystemMetadata()).thenReturn(systemMetadata);

    when(mockEntityService.alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            Pair.<Future<?>, Boolean>of(CompletableFuture.completedFuture(null), Boolean.TRUE));

    List<Urn> callbackUrns = new ArrayList<>();

    try (MockedStatic<EntityUtils> entityUtils = mockStatic(EntityUtils.class)) {
      entityUtils
          .when(() -> EntityUtils.toSystemAspectFromEbeanAspects(any(), any(), any()))
          .thenReturn(List.of(systemAspect));

      RestoreIndicesStreamUtil.reindexAspect(
          mockOpContext,
          mockEntityService,
          mockAspectDao,
          "upstreamLineage",
          "urn:li:dataset:%",
          10,
          0,
          0,
          ChangeType.RESTATE,
          "run-42",
          null,
          callbackUrns::add);
    }

    verify(mockEntityService)
        .alwaysProduceMCLAsync(
            eq(mockOpContext),
            eq(urn),
            eq(urn.getEntityType()),
            eq("upstreamLineage"),
            any(),
            isNull(),
            any(),
            isNull(),
            eq(systemMetadata),
            any(AuditStamp.class),
            eq(ChangeType.RESTATE));
    assertEquals(callbackUrns, List.of(urn));
    assertEquals(systemMetadata.getRunId(), "run-42");
  }
}
