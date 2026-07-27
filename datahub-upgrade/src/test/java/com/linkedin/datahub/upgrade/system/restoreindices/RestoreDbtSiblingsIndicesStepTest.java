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
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
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
  @Mock private OperationContext mockOpContext;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private EntityRegistry mockEntityRegistry;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
  }

  @Test
  public void testIsOptional() {
    RestoreDbtSiblingsIndicesStep step = new RestoreDbtSiblingsIndicesStep(mockEntityService, true);
    assertTrue(step.isOptional());
  }

  @Test
  public void testSkipsWhenDisabled() {
    RestoreDbtSiblingsIndicesStep step =
        new RestoreDbtSiblingsIndicesStep(mockEntityService, false);
    assertTrue(step.skip(mockUpgradeContext));
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

    RestoreDbtSiblingsIndicesStep step = new RestoreDbtSiblingsIndicesStep(mockEntityService, true);
    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testDoesNotSkipWhenNotPreviouslyRun() {
    when(mockEntityService.exists(
            eq(mockOpContext),
            eq(SIBLING_UPGRADE_URN),
            eq(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(false);

    RestoreDbtSiblingsIndicesStep step = new RestoreDbtSiblingsIndicesStep(mockEntityService, true);
    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSuccessfulExecutionWithNoDatasets() throws Exception {
    ListUrnsResult mockListResult = mock(ListUrnsResult.class);
    when(mockListResult.getTotal()).thenReturn(0);
    when(mockEntityService.listUrns(
            eq(mockOpContext), eq(Constants.DATASET_ENTITY_NAME), eq(0), eq(10)))
        .thenReturn(mockListResult);
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockEntitySpec.getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME))
        .thenReturn(mockAspectSpec);
    when(mockEntityRegistry.getEntitySpec(Constants.DATASET_ENTITY_NAME))
        .thenReturn(mockEntitySpec);

    RestoreDbtSiblingsIndicesStep step = new RestoreDbtSiblingsIndicesStep(mockEntityService, true);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

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
  public void testFailureHandling() throws Exception {
    when(mockEntityService.listUrns(any(), anyString(), anyInt(), anyInt()))
        .thenThrow(new RuntimeException("Test exception"));
    RestoreDbtSiblingsIndicesStep step = new RestoreDbtSiblingsIndicesStep(mockEntityService, true);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockEntityService, times(1)).deleteUrn(eq(mockOpContext), eq(SIBLING_UPGRADE_URN));
  }
}
