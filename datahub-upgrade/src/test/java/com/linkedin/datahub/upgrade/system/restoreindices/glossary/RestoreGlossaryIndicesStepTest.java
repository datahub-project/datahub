package com.linkedin.datahub.upgrade.system.restoreindices.glossary;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.restoreindices.RestoreIndicesTestHelpers;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestoreGlossaryIndicesStepTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private AspectDao mockAspectDao;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private OperationContext mockOpContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
  }

  @Test
  public void testSkipReturnsFalseWhenNoPriorRun() throws Exception {
    when(mockEntityService.getEntityV2(any(), any(), any(), any())).thenReturn(null);
    assertFalse(newStep().skip(mockUpgradeContext));
  }

  @Test
  public void testSkipReturnsTrueWhenAlreadyRan() throws Exception {
    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME), any(), any()))
        .thenReturn(RestoreIndicesTestHelpers.upgradeRequestResponse("1"));
    assertTrue(newStep().skip(mockUpgradeContext));
  }

  @Test
  public void testSkipReturnsFalseWhenVersionMismatch() throws Exception {
    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME), any(), any()))
        .thenReturn(RestoreIndicesTestHelpers.upgradeRequestResponse("0"));
    assertFalse(newStep().skip(mockUpgradeContext));
  }

  /** Reindexes both glossaryTermInfo (terms) and glossaryNodeInfo (nodes) via streaming. */
  @Test
  public void testExecutableScansTermAndNodeAspects() {
    PartitionedStream<EbeanAspectV2> emptyStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(any(), any(RestoreIndicesArgs.class)))
        .thenReturn(emptyStream);
    when(emptyStream.partition(anyInt())).thenAnswer(invocation -> Stream.empty());

    UpgradeStepResult result = newStep().executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<RestoreIndicesArgs> captor = ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao, times(2)).streamAspectBatches(any(), captor.capture());
    List<RestoreIndicesArgs> scans = captor.getAllValues();
    assertTrue(
        RestoreIndicesTestHelpers.scanContains(
            scans, Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, "urn:li:glossaryTerm:%"));
    assertTrue(
        RestoreIndicesTestHelpers.scanContains(
            scans, Constants.GLOSSARY_NODE_INFO_ASPECT_NAME, "urn:li:glossaryNode:%"));

    verify(mockEntityService, never())
        .alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(mockEntityService)
        .ingestProposal(
            any(),
            argThat(p -> Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME.equals(p.getAspectName())),
            any(AuditStamp.class),
            eq(false));
    verify(mockEntityService)
        .ingestProposal(
            any(),
            argThat(p -> Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME.equals(p.getAspectName())),
            any(AuditStamp.class),
            eq(false));
  }

  @Test
  public void testExecutableReturnsFailedOnException() {
    when(mockAspectDao.streamAspectBatches(any(), any(RestoreIndicesArgs.class)))
        .thenThrow(new RuntimeException("db error"));

    UpgradeStepResult result = newStep().executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockEntityService).deleteUrn(any(), any(Urn.class));
  }

  private RestoreGlossaryIndicesStep newStep() {
    return new RestoreGlossaryIndicesStep(mockEntityService, mockAspectDao);
  }
}
