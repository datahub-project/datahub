package com.linkedin.datahub.upgrade.system.policyprivileges;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BackfillViewAllQueriesPrivilegeStepTest {

  private static final String VIEW_ENTITY_PAGE =
      PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE.getType();
  private static final String VIEW_ALL_QUERIES =
      PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE.getType();

  private static final Urn POLICY_URN = UrnUtils.getUrn("urn:li:dataHubPolicy:test-policy");

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Mock private EntityService<?> mockEntityService;
  @Mock private SearchService mockSearchService;
  @Mock private UpgradeContext mockUpgradeContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(opContext);
  }

  private static DataHubPolicyInfo policyInfo(String... privileges) {
    final DataHubPolicyInfo info = new DataHubPolicyInfo();
    info.setDisplayName("Test Policy");
    info.setType("METADATA");
    info.setState("ACTIVE");
    info.setPrivileges(new StringArray(java.util.Arrays.asList(privileges)));
    return info;
  }

  @Test
  public void testShouldBackfillOnlyPoliciesGrantingEntityPageWithoutQueries() {
    assertTrue(BackfillViewAllQueriesPrivilegeStep.shouldBackfill(policyInfo(VIEW_ENTITY_PAGE)));
    assertFalse(
        BackfillViewAllQueriesPrivilegeStep.shouldBackfill(
            policyInfo(VIEW_ENTITY_PAGE, VIEW_ALL_QUERIES)),
        "already granted — must be idempotent");
    assertFalse(
        BackfillViewAllQueriesPrivilegeStep.shouldBackfill(policyInfo("EDIT_ENTITY_TAGS")),
        "policies that don't grant entity-page view are untouched");
    assertFalse(
        BackfillViewAllQueriesPrivilegeStep.shouldBackfill(new DataHubPolicyInfo()),
        "policies without privileges are untouched");
  }

  @Test
  public void testSkipWhenPreviouslyRun() {
    when(mockEntityService.exists(
            any(OperationContext.class),
            any(Urn.class),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            anyBoolean()))
        .thenReturn(true);

    BackfillViewAllQueriesPrivilegeStep step =
        new BackfillViewAllQueriesPrivilegeStep(
            opContext, mockEntityService, mockSearchService, false, 100);
    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testNoSkipWhenReprocessEnabled() {
    when(mockEntityService.exists(
            any(OperationContext.class),
            any(Urn.class),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            anyBoolean()))
        .thenReturn(true);

    BackfillViewAllQueriesPrivilegeStep step =
        new BackfillViewAllQueriesPrivilegeStep(
            opContext, mockEntityService, mockSearchService, true, 100);
    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableBackfillsMatchingPolicy() throws Exception {
    mockScrollWithSinglePolicy();
    mockPolicyResponse(policyInfo(VIEW_ENTITY_PAGE, "EDIT_ENTITY_TAGS"));

    BackfillViewAllQueriesPrivilegeStep step =
        new BackfillViewAllQueriesPrivilegeStep(
            opContext, mockEntityService, mockSearchService, false, 100);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // setUpgradeResult also flows through ingestProposal — filter to the policy-info proposal
    List<MetadataChangeProposal> policyProposals = capturePolicyInfoProposals();
    assertEquals(policyProposals.size(), 1, "exactly one policy should be backfilled");

    MetadataChangeProposal proposal = policyProposals.get(0);
    assertEquals(proposal.getEntityUrn(), POLICY_URN);
    String serialized = proposal.getAspect().getValue().asString("UTF-8");
    assertTrue(serialized.contains(VIEW_ALL_QUERIES), "backfilled privilege must be present");
    assertTrue(serialized.contains(VIEW_ENTITY_PAGE), "existing privileges must be preserved");
  }

  @Test
  public void testExecutableLeavesAlreadyGrantedPolicyAlone() throws Exception {
    mockScrollWithSinglePolicy();
    mockPolicyResponse(policyInfo(VIEW_ENTITY_PAGE, VIEW_ALL_QUERIES));

    BackfillViewAllQueriesPrivilegeStep step =
        new BackfillViewAllQueriesPrivilegeStep(
            opContext, mockEntityService, mockSearchService, false, 100);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    assertTrue(
        capturePolicyInfoProposals().isEmpty(),
        "already-granted policy must not be re-ingested (only the upgrade marker may be written)");
  }

  @Test
  public void testExecutableLeavesMarkerUnwrittenAndFailsWhenAPolicyFailsToIngest()
      throws Exception {
    mockScrollWithSinglePolicy();
    mockPolicyResponse(policyInfo(VIEW_ENTITY_PAGE, "EDIT_ENTITY_TAGS"));
    when(mockEntityService.ingestProposal(
            any(OperationContext.class),
            argThat(p -> DATAHUB_POLICY_INFO_ASPECT_NAME.equals(p.getAspectName())),
            any(),
            anyBoolean()))
        .thenThrow(new RuntimeException("simulated ingest failure"));

    BackfillViewAllQueriesPrivilegeStep step =
        new BackfillViewAllQueriesPrivilegeStep(
            opContext, mockEntityService, mockSearchService, false, 100);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(
        result.result(),
        DataHubUpgradeState.FAILED,
        "a failed policy ingest must fail the step, not silently succeed");
    verify(mockEntityService, never())
        .ingestProposal(
            any(OperationContext.class),
            argThat(p -> DATA_HUB_UPGRADE_RESULT_ASPECT_NAME.equals(p.getAspectName())),
            any(),
            anyBoolean());
  }

  /** Captures all ingested proposals and returns only the dataHubPolicyInfo ones. */
  private List<MetadataChangeProposal> capturePolicyInfoProposals() {
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService, atLeast(0))
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture(), any(), anyBoolean());
    return proposalCaptor.getAllValues().stream()
        .filter(p -> DATAHUB_POLICY_INFO_ASPECT_NAME.equals(p.getAspectName()))
        .collect(Collectors.toList());
  }

  private void mockScrollWithSinglePolicy() {
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setNumEntities(1);
    scrollResult.setEntities(
        new SearchEntityArray(java.util.List.of(new SearchEntity().setEntity(POLICY_URN))));
    // no scrollId → single page
    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class),
            eq(com.google.common.collect.ImmutableList.of(Constants.POLICY_ENTITY_NAME)),
            eq("*"),
            any(),
            any(),
            any(),
            any(),
            anyInt()))
        .thenReturn(scrollResult);
  }

  private void mockPolicyResponse(DataHubPolicyInfo info) throws Exception {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(info.data()));
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(DATAHUB_POLICY_INFO_ASPECT_NAME, envelopedAspect);
    EntityResponse response = new EntityResponse();
    response.setUrn(POLICY_URN);
    response.setAspects(aspectMap);
    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(POLICY_URN.getEntityType()),
            eq(java.util.Collections.singleton(POLICY_URN)),
            eq(java.util.Collections.singleton(DATAHUB_POLICY_INFO_ASPECT_NAME))))
        .thenReturn(java.util.Collections.singletonMap(POLICY_URN, response));
    // ingestProposal returning null IngestResult is fine for these tests
    when(mockEntityService.ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), any(), anyBoolean()))
        .thenReturn(null);
  }
}
