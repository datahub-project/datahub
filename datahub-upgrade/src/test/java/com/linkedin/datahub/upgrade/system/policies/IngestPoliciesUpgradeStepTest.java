package com.linkedin.datahub.upgrade.system.policies;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestPoliciesUpgradeStepTest {

  private static final String NON_EDITABLE_POLICY_JSON =
      "[{\"urn\":\"urn:li:dataHubPolicy:test\","
          + "\"info\":{\"type\":\"METADATA\",\"state\":\"ACTIVE\",\"editable\":false,"
          + "\"actors\":{\"allUsers\":true},\"privileges\":[],\"displayName\":\"Test\"}}]";

  private static final String POLICY_WITHOUT_INFO_JSON =
      "[{\"urn\":\"urn:li:dataHubPolicy:test\"}]";

  @Mock private EntityService<?> mockEntityService;
  @Mock private EntitySearchService mockEntitySearchService;
  @Mock private SearchDocumentTransformer mockSearchDocumentTransformer;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private OperationContext mockOpContext;
  @Mock private RetrieverContext mockRetrieverContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
  }

  @Test
  public void testSkipWhenDisabled() {
    Resource resource = new ByteArrayResource(NON_EDITABLE_POLICY_JSON.getBytes());
    IngestPoliciesUpgradeStep step =
        new IngestPoliciesUpgradeStep(
            mockEntityService,
            mockEntitySearchService,
            mockSearchDocumentTransformer,
            resource,
            false);

    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testNoSkipWhenEnabled() {
    Resource resource = new ByteArrayResource(NON_EDITABLE_POLICY_JSON.getBytes());
    IngestPoliciesUpgradeStep step =
        new IngestPoliciesUpgradeStep(
            mockEntityService,
            mockEntitySearchService,
            mockSearchDocumentTransformer,
            resource,
            true);

    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableIngestsNonEditablePolicy() throws Exception {
    Resource resource = new ByteArrayResource(NON_EDITABLE_POLICY_JSON.getBytes());
    IngestPoliciesUpgradeStep step =
        new IngestPoliciesUpgradeStep(
            mockEntityService,
            mockEntitySearchService,
            mockSearchDocumentTransformer,
            resource,
            true);

    // docCount returns > 0 so updatePolicyIndex is skipped
    when(mockEntitySearchService.docCount(any(), any())).thenReturn(1L);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // ingestProposal called for key aspect + policy info aspect (2 proposals in one batch)
    verify(mockEntityService).ingestProposal(any(OperationContext.class), any(), eq(false));
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableDeletesPolicyWithNoInfo() throws Exception {
    Resource resource = new ByteArrayResource(POLICY_WITHOUT_INFO_JSON.getBytes());
    IngestPoliciesUpgradeStep step =
        new IngestPoliciesUpgradeStep(
            mockEntityService,
            mockEntitySearchService,
            mockSearchDocumentTransformer,
            resource,
            true);

    when(mockEntitySearchService.docCount(any(), any())).thenReturn(1L);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService).deleteUrn(any(OperationContext.class), any(Urn.class));
    verify(mockEntityService, never()).ingestProposal(any(), any(), eq(false));
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableFailsOnException() {
    Resource resource = new ByteArrayResource(NON_EDITABLE_POLICY_JSON.getBytes());
    IngestPoliciesUpgradeStep step =
        new IngestPoliciesUpgradeStep(
            mockEntityService,
            mockEntitySearchService,
            mockSearchDocumentTransformer,
            resource,
            true);

    when(mockEntityService.ingestProposal(any(), any(), eq(false)))
        .thenThrow(new RuntimeException("simulated failure"));

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }
}
