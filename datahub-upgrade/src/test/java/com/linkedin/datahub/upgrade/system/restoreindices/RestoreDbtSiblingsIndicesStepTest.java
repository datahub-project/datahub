package com.linkedin.datahub.upgrade.system.restoreindices;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
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
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestoreDbtSiblingsIndicesStepTest {

  private static final Urn SIBLING_UPGRADE_URN =
      EntityKeyUtils.convertEntityKeyToUrn(
          new DataHubUpgradeKey().setId("restore-dbt-siblings-indices"),
          Constants.DATA_HUB_UPGRADE_ENTITY_NAME);

  @Mock private EntityService<?> mockEntityService;
  @Mock private AspectDao mockAspectDao;
  @Mock private OperationContext mockOpContext;
  @Mock private UpgradeContext mockUpgradeContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
  }

  @Test
  public void testIsOptional() {
    assertTrue(newStep(true).isOptional());
  }

  @Test
  public void testSkipsWhenDisabled() {
    assertTrue(newStep(false).skip(mockUpgradeContext));
    verify(mockEntityService, never()).exists(any(), any(Urn.class), anyString(), anyBoolean());
  }

  @Test
  public void testSkipsWhenAlreadyRun() {
    when(mockEntityService.exists(
            eq(mockOpContext),
            eq(SIBLING_UPGRADE_URN),
            eq(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(true);
    assertTrue(newStep(true).skip(mockUpgradeContext));
  }

  @Test
  public void testDoesNotSkipWhenNotPreviouslyRun() {
    when(mockEntityService.exists(
            eq(mockOpContext),
            eq(SIBLING_UPGRADE_URN),
            eq(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(false);
    assertFalse(newStep(true).skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableScansUpstreamLineageAndSucceeds() {
    PartitionedStream<EbeanAspectV2> emptyStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(any(), any(RestoreIndicesArgs.class)))
        .thenReturn(emptyStream);
    when(emptyStream.partition(anyInt())).thenReturn(Stream.empty());

    UpgradeStepResult result = newStep(true).executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<RestoreIndicesArgs> scanCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao).streamAspectBatches(any(), scanCaptor.capture());
    assertEquals(scanCaptor.getValue().aspectName(), Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    assertEquals(scanCaptor.getValue().urnLike(), "urn:li:dataset:%");

    // Request marker then result marker are recorded against the upgrade urn.
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService, times(2))
        .ingestProposal(
            eq(mockOpContext), proposalCaptor.capture(), any(AuditStamp.class), eq(false));
    List<MetadataChangeProposal> proposals = proposalCaptor.getAllValues();
    assertEquals(proposals.get(0).getAspectName(), Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME);
    assertEquals(proposals.get(0).getEntityUrn(), SIBLING_UPGRADE_URN);
    assertEquals(proposals.get(1).getAspectName(), Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    assertEquals(proposals.get(1).getEntityUrn(), SIBLING_UPGRADE_URN);
  }

  @Test
  public void testFailureHandling() {
    when(mockAspectDao.streamAspectBatches(any(), any(RestoreIndicesArgs.class)))
        .thenThrow(new RuntimeException("Test exception"));

    UpgradeStepResult result = newStep(true).executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockEntityService, times(1)).deleteUrn(eq(mockOpContext), eq(SIBLING_UPGRADE_URN));
  }

  private RestoreDbtSiblingsIndicesStep newStep(boolean enabled) {
    return new RestoreDbtSiblingsIndicesStep(mockEntityService, mockAspectDao, enabled);
  }
}
