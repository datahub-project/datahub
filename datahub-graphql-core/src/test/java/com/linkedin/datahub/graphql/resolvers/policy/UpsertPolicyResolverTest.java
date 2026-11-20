package com.linkedin.datahub.graphql.resolvers.policy;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ActorFilterInput;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.datahub.graphql.generated.PolicyUpdateInput;
import com.linkedin.datahub.graphql.generated.ResourceFilterInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Regression tests for https://github.com/datahub-project/datahub/issues/19909: editing a policy
 * through {@link UpsertPolicyResolver} silently dropped {@code actors.roles} and reset {@code
 * editable}, because {@link PolicyUpdateInput} / {@link ActorFilterInput} have no representation
 * for those fields, and the resolver reconstructs the {@link DataHubPolicyInfo} aspect from scratch
 * before ingesting it as a full replace.
 */
public class UpsertPolicyResolverTest {

  private static final String POLICY_URN = "urn:li:dataHubPolicy:editor-metadata-policy-entities";
  private static final String ACTOR_URN = "urn:li:corpuser:test";
  private static final String EDITOR_ROLE_URN = "urn:li:dataHubRole:Editor";

  private EntityClient entityClient;
  private UpsertPolicyResolver resolver;
  private DataFetchingEnvironment environment;
  private QueryContext context;
  private OperationContext operationContext;
  private MockedStatic<ResolverUtils> resolverUtils;
  private MockedStatic<PolicyAuthUtils> policyAuthUtils;
  private MockedStatic<GraphQLConcurrencyUtils> concurrencyUtils;

  @BeforeMethod
  public void setup() {
    entityClient = mock(EntityClient.class);
    resolver = new UpsertPolicyResolver(entityClient);
    environment = mock(DataFetchingEnvironment.class);
    context = mock(QueryContext.class);
    operationContext = mock(OperationContext.class);
    AuthorizerChain authorizerChain = mock(AuthorizerChain.class);
    DataHubAuthorizer dataHubAuthorizer = mock(DataHubAuthorizer.class);

    resolverUtils = mockStatic(ResolverUtils.class);
    policyAuthUtils = mockStatic(PolicyAuthUtils.class);
    concurrencyUtils = mockStatic(GraphQLConcurrencyUtils.class);

    when(environment.getContext()).thenReturn(context);
    when(context.getOperationContext()).thenReturn(operationContext);
    when(context.getActorUrn()).thenReturn(ACTOR_URN);
    when(context.getAuthorizer()).thenReturn(authorizerChain);
    when(authorizerChain.getDefaultAuthorizer()).thenReturn(dataHubAuthorizer);
    policyAuthUtils.when(() -> PolicyAuthUtils.canManagePolicies(context)).thenReturn(true);

    concurrencyUtils
        .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              java.util.function.Supplier<?> supplier = invocation.getArgument(0);
              return CompletableFuture.completedFuture(supplier.get());
            });
  }

  @AfterMethod
  public void tearDown() {
    resolverUtils.close();
    policyAuthUtils.close();
    concurrencyUtils.close();
  }

  @Test
  public void testUpdatePolicyPreservesRolesAndEditable() throws Exception {
    // Existing persisted aspect: an editable built-in policy with the Editor role assigned.
    final DataHubPolicyInfo existingInfo = new DataHubPolicyInfo();
    existingInfo.setDisplayName("Editors - Edit Metadata Entities");
    existingInfo.setType("METADATA");
    existingInfo.setState("ACTIVE");
    existingInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    existingInfo.setEditable(true);
    existingInfo.setActors(
        new DataHubActorFilter()
            .setAllUsers(false)
            .setAllGroups(false)
            .setResourceOwners(false)
            .setRoles(new UrnArray(UrnUtils.getUrn(EDITOR_ROLE_URN))));

    final EnvelopedAspect envelopedAspect =
        new EnvelopedAspect().setValue(new Aspect(existingInfo.data()));
    final EntityResponse existingResponse = new EntityResponse();
    existingResponse.setUrn(UrnUtils.getUrn(POLICY_URN));
    existingResponse.setAspects(
        new EnvelopedAspectMap(java.util.Map.of("dataHubPolicyInfo", envelopedAspect)));

    when(entityClient.getV2(
            eq(operationContext), eq("dataHubPolicy"), eq(UrnUtils.getUrn(POLICY_URN)), any()))
        .thenReturn(existingResponse);
    when(entityClient.ingestProposal(
            eq(operationContext), any(MetadataChangeProposal.class), eq(false)))
        .thenReturn(POLICY_URN);

    // The edit form only sends fields that PolicyUpdateInput actually models - no roles, no
    // editable - mirroring what the frontend's toPolicyInput() constructs today.
    final ActorFilterInput actorInput = new ActorFilterInput();
    actorInput.setAllUsers(false);
    actorInput.setAllGroups(false);
    actorInput.setResourceOwners(false);

    final PolicyUpdateInput input = new PolicyUpdateInput();
    input.setType(PolicyType.METADATA);
    input.setName("Editors - Edit Metadata Entities");
    input.setState(PolicyState.ACTIVE);
    input.setDescription("desc");
    input.setPrivileges(java.util.List.of("EDIT_ENTITY_TAGS", "EDIT_ENTITY_DOCS"));
    input.setActors(actorInput);

    when(environment.getArgument("urn")).thenReturn(POLICY_URN);
    when(environment.getArgument("input")).thenReturn(input);
    resolverUtils.when(() -> bindArgument(input, PolicyUpdateInput.class)).thenReturn(input);

    final String result = resolver.get(environment).get();
    assertEquals(result, POLICY_URN);

    final ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityClient).ingestProposal(eq(operationContext), proposalCaptor.capture(), eq(false));

    final DataHubPolicyInfo persistedInfo =
        GenericRecordUtils.deserializeAspect(
            proposalCaptor.getValue().getAspect().getValue(),
            proposalCaptor.getValue().getAspect().getContentType(),
            DataHubPolicyInfo.class);

    // The bug: these used to come back empty/reset even though the input never touched them.
    assertTrue(persistedInfo.getActors().hasRoles());
    assertEquals(
        persistedInfo.getActors().getRoles(), new UrnArray(UrnUtils.getUrn(EDITOR_ROLE_URN)));
    assertTrue(persistedInfo.isEditable());
    // Sanity check that the fields actually present on the input still get applied.
    assertEquals(
        persistedInfo.getPrivileges(), new StringArray("EDIT_ENTITY_TAGS", "EDIT_ENTITY_DOCS"));
  }

  @Test
  public void testUpdatePolicyFromUiKeepsPrivilegeConstraints() throws Exception {
    // What graphql-java hands the resolver for a UI save: `resources` without the key.
    final DataHubPolicyInfo persistedInfo = updateConstrainedPolicy(Map.of("allResources", true));
    assertEquals(
        persistedInfo.getResources().getPrivilegeConstraints().getCriteria().get(0).getValues(),
        new StringArray("urn:li:tag:pii"));
  }

  @Test
  public void testUpdatePolicyWithExplicitNullClearsPrivilegeConstraints() throws Exception {
    final Map<String, Object> rawResources = new HashMap<>();
    rawResources.put("allResources", true);
    rawResources.put("privilegeConstraints", null);
    final DataHubPolicyInfo persistedInfo = updateConstrainedPolicy(rawResources);
    assertFalse(persistedInfo.getResources().hasPrivilegeConstraints());
  }

  /** Updates a policy stored with a tag constraint, sending {@code rawResources} as input. */
  private DataHubPolicyInfo updateConstrainedPolicy(final Map<String, Object> rawResources)
      throws Exception {
    final PolicyMatchFilter constraints =
        new PolicyMatchFilter()
            .setCriteria(
                new PolicyMatchCriterionArray(
                    new PolicyMatchCriterion()
                        .setField("TAG")
                        .setValues(new StringArray("urn:li:tag:pii"))
                        .setCondition(PolicyMatchCondition.NOT_EQUALS)));
    final DataHubPolicyInfo existingInfo = new DataHubPolicyInfo();
    existingInfo.setDisplayName("Tag editors");
    existingInfo.setType("METADATA");
    existingInfo.setState("ACTIVE");
    existingInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    existingInfo.setActors(new DataHubActorFilter().setAllUsers(true));
    existingInfo.setResources(
        new DataHubResourceFilter().setAllResources(true).setPrivilegeConstraints(constraints));

    final EntityResponse existingResponse = new EntityResponse();
    existingResponse.setUrn(UrnUtils.getUrn(POLICY_URN));
    existingResponse.setAspects(
        new EnvelopedAspectMap(
            Map.of(
                "dataHubPolicyInfo",
                new EnvelopedAspect().setValue(new Aspect(existingInfo.data())))));
    when(entityClient.getV2(
            eq(operationContext), eq("dataHubPolicy"), eq(UrnUtils.getUrn(POLICY_URN)), any()))
        .thenReturn(existingResponse);
    when(entityClient.ingestProposal(
            eq(operationContext), any(MetadataChangeProposal.class), eq(false)))
        .thenReturn(POLICY_URN);

    final ActorFilterInput actorInput = new ActorFilterInput();
    actorInput.setAllUsers(true);
    actorInput.setAllGroups(false);
    actorInput.setResourceOwners(false);
    final ResourceFilterInput resourcesInput = new ResourceFilterInput();
    resourcesInput.setAllResources(true);
    final PolicyUpdateInput input = new PolicyUpdateInput();
    input.setType(PolicyType.METADATA);
    input.setName("Tag editors");
    input.setState(PolicyState.ACTIVE);
    input.setPrivileges(List.of("EDIT_ENTITY_TAGS"));
    input.setActors(actorInput);
    input.setResources(resourcesInput);

    final Map<String, Object> rawInput = Map.of("name", "Tag editors", "resources", rawResources);
    when(environment.getArgument("urn")).thenReturn(POLICY_URN);
    when(environment.getArgument("input")).thenReturn(rawInput);
    resolverUtils.when(() -> bindArgument(rawInput, PolicyUpdateInput.class)).thenReturn(input);

    resolver.get(environment).get();

    final ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityClient).ingestProposal(eq(operationContext), proposalCaptor.capture(), eq(false));
    return GenericRecordUtils.deserializeAspect(
        proposalCaptor.getValue().getAspect().getValue(),
        proposalCaptor.getValue().getAspect().getContentType(),
        DataHubPolicyInfo.class);
  }
}
