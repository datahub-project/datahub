package com.linkedin.datahub.upgrade.system.policies;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RetrieverContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestPoliciesUpgradeStepTest {

  private static final String EDITABLE_POLICY_JSON =
      "[{\"urn\":\"urn:li:dataHubPolicy:test\","
          + "\"info\":{\"type\":\"METADATA\",\"state\":\"ACTIVE\",\"editable\":true,"
          + "\"actors\":{\"allUsers\":true},\"privileges\":[],\"displayName\":\"Test\"}}]";

  private static final String NON_EDITABLE_POLICY_JSON =
      "[{\"urn\":\"urn:li:dataHubPolicy:test\","
          + "\"info\":{\"type\":\"METADATA\",\"state\":\"ACTIVE\",\"editable\":false,"
          + "\"actors\":{\"allUsers\":true},\"privileges\":[],\"displayName\":\"Test\"}}]";

  private static final String POLICY_WITHOUT_INFO_JSON =
      "[{\"urn\":\"urn:li:dataHubPolicy:test\"}]";

  private static final String EDITABLE_POLICY_WITH_VIEW_ALL_QUERIES_JSON =
      "[{\"urn\":\"urn:li:dataHubPolicy:test\","
          + "\"info\":{\"type\":\"METADATA\",\"state\":\"ACTIVE\",\"editable\":true,"
          + "\"actors\":{\"allUsers\":true},"
          + "\"privileges\":[\"VIEW_ENTITY_PAGE\",\"VIEW_ALL_QUERIES\"],\"displayName\":\"Test\","
          + "\"description\":\"Test\"}}]";

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
  public void testExecutableIngestsNonEditablePolicy() {
    Resource resource = new ByteArrayResource(NON_EDITABLE_POLICY_JSON.getBytes());
    IngestPoliciesUpgradeStep step =
        new IngestPoliciesUpgradeStep(
            mockEntityService,
            mockEntitySearchService,
            mockSearchDocumentTransformer,
            resource,
            true);

    when(mockEntitySearchService.docCount(any(), any())).thenReturn(1L);
    step.executable().apply(mockUpgradeContext);

    // Non-editable: hasPolicy check skipped — ingestPolicy always attempted without existence check
    verify(mockEntityService, never()).getAspect(any(), any(), any(), eq(0));
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
  public void testExecutableSkipsEditablePolicyWhenExists() throws Exception {
    Resource resource = new ByteArrayResource(EDITABLE_POLICY_JSON.getBytes());
    IngestPoliciesUpgradeStep step =
        new IngestPoliciesUpgradeStep(
            mockEntityService,
            mockEntitySearchService,
            mockSearchDocumentTransformer,
            resource,
            true);

    // Policy already exists — getAspect returns non-null
    when(mockEntityService.getAspect(any(), any(), any(), eq(0L)))
        .thenReturn(new DataHubPolicyInfo());
    when(mockEntitySearchService.docCount(any(), any())).thenReturn(1L);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService, never()).ingestProposal(any(), any(), eq(false));
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  /**
   * A fresh install with VIEW_AUTHORIZATION_ENABLED=true must not seed a stock editable default
   * (e.g. "All Users") with the broad, unconditional VIEW_ALL_QUERIES privilege — mirrors the
   * upgrade-side BackfillViewAllQueriesPrivilegeStep restriction to non-editable (system) policies.
   */
  @Test
  public void testExecutableStripsViewAllQueriesFromFreshEditablePolicyWhenViewAuthEnabled()
      throws Exception {
    Resource resource =
        new ByteArrayResource(EDITABLE_POLICY_WITH_VIEW_ALL_QUERIES_JSON.getBytes());
    IngestPoliciesUpgradeStep step =
        new IngestPoliciesUpgradeStep(
            mockEntityService,
            mockEntitySearchService,
            mockSearchDocumentTransformer,
            resource,
            true);

    // Fresh install: policy does not already exist.
    when(mockEntityService.getAspect(any(), any(), any(), eq(0L))).thenReturn(null);
    when(mockEntitySearchService.docCount(any(), any())).thenReturn(1L);
    when(mockUpgradeContext.opContext()).thenReturn(opContextWithViewAuthEnabled(true));

    step.executable().apply(mockUpgradeContext);

    String serialized = capturePolicyInfoProposalJson();
    assertFalse(
        serialized.contains("VIEW_ALL_QUERIES"),
        "VIEW_ALL_QUERIES must be stripped when VIEW_AUTHORIZATION_ENABLED is on");
    assertTrue(
        serialized.contains("VIEW_ENTITY_PAGE"), "other privileges must be preserved unchanged");
  }

  /** Same fresh-install case, but with VIEW_AUTHORIZATION_ENABLED off: nothing is stripped. */
  @Test
  public void testExecutablePreservesViewAllQueriesFromFreshEditablePolicyWhenViewAuthDisabled()
      throws Exception {
    Resource resource =
        new ByteArrayResource(EDITABLE_POLICY_WITH_VIEW_ALL_QUERIES_JSON.getBytes());
    IngestPoliciesUpgradeStep step =
        new IngestPoliciesUpgradeStep(
            mockEntityService,
            mockEntitySearchService,
            mockSearchDocumentTransformer,
            resource,
            true);

    when(mockEntityService.getAspect(any(), any(), any(), eq(0L))).thenReturn(null);
    when(mockEntitySearchService.docCount(any(), any())).thenReturn(1L);
    when(mockUpgradeContext.opContext()).thenReturn(opContextWithViewAuthEnabled(false));

    step.executable().apply(mockUpgradeContext);

    String serialized = capturePolicyInfoProposalJson();
    assertTrue(
        serialized.contains("VIEW_ALL_QUERIES"),
        "VIEW_ALL_QUERIES must be preserved when VIEW_AUTHORIZATION_ENABLED is off");
  }

  /**
   * A real, fully-functional OperationContext (not the bare {@code mockOpContext}) with a specific
   * VIEW_AUTHORIZATION_ENABLED state — {@code ingestPolicy} needs a working entity registry to
   * resolve the policy key aspect spec, which a bare mock doesn't provide.
   */
  private OperationContext opContextWithViewAuthEnabled(boolean enabled) {
    return io.datahubproject.test.metadata.context.TestOperationContexts.systemContext(
        () ->
            OperationContextConfig.builder()
                .viewAuthorizationConfiguration(
                    ViewAuthorizationConfiguration.builder().enabled(enabled).build())
                .build(),
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  /** Captures the ingested policy-info proposal's serialized aspect as a UTF-8 string. */
  private String capturePolicyInfoProposalJson() {
    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService).ingestProposal(any(), batchCaptor.capture(), eq(false));
    return batchCaptor.getValue().getMCPItems().stream()
        .map(item -> item.getMetadataChangeProposal())
        .filter(mcp -> Constants.DATAHUB_POLICY_INFO_ASPECT_NAME.equals(mcp.getAspectName()))
        .findFirst()
        .orElseThrow()
        .getAspect()
        .getValue()
        .asString("UTF-8");
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
