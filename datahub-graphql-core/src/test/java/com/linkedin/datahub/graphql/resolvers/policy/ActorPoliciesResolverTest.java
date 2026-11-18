package com.linkedin.datahub.graphql.resolvers.policy;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.authorization.ActorPolicies;
import com.datahub.authorization.ActorPolicyMatch;
import com.datahub.authorization.ActorPolicyMatchReason;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.ActorPoliciesResult;
import com.linkedin.datahub.graphql.generated.ActorPoliciesView;
import com.linkedin.datahub.graphql.generated.ActorPolicy;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.PolicyMatchReasonType;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ActorPoliciesResolverTest {

  private static final Urn CALLER = UrnUtils.getUrn("urn:li:corpuser:caller");
  private static final Urn OTHER_USER = UrnUtils.getUrn("urn:li:corpuser:other");
  private static final Urn GROUP = UrnUtils.getUrn("urn:li:corpGroup:team");
  private static final Urn POLICY = UrnUtils.getUrn("urn:li:dataHubPolicy:p1");

  private DataHubAuthorizer authorizer;
  private AuthorizerChain chain;
  private QueryContext context;
  private FeatureFlags featureFlags;

  @BeforeMethod
  public void setup() {
    authorizer = mock(DataHubAuthorizer.class);
    chain = mock(AuthorizerChain.class);
    when(chain.getDefaultAuthorizer()).thenReturn(authorizer);
    setAdmin(false);
    featureFlags = new FeatureFlags();
    when(authorizer.getActorGroupUrns(any(), eq(CALLER))).thenReturn(Set.of());
    when(authorizer.explainUserPolicies(any(), any(), anyBoolean()))
        .thenReturn(new ActorPolicies(List.of(policyMatch()), List.of()));
    when(authorizer.explainGroupPolicies(any(), any(), anyBoolean()))
        .thenReturn(new ActorPolicies(List.of(policyMatch()), List.of()));
  }

  @Test
  public void adminUserFull() throws Exception {
    setAdmin(true);
    final ActorPoliciesResult result = resolve(user(OTHER_USER), Map.of("includeInactive", true));
    assertEquals(result.getView(), ActorPoliciesView.FULL);
    assertNotNull(result.getPolicies().get(0).getActors());
    verify(authorizer).explainUserPolicies(any(), eq(OTHER_USER), eq(true));
  }

  @Test
  public void adminGroupFull() throws Exception {
    setAdmin(true);
    featureFlags.setActorPoliciesSelfServiceEnabled(false);
    final ActorPoliciesResult result = resolve(group(GROUP), null);
    assertEquals(result.getView(), ActorPoliciesView.FULL);
    assertNotNull(result.getPolicies().get(0).getActors());
    verify(authorizer).explainGroupPolicies(any(), eq(GROUP), eq(false));
  }

  @Test
  public void selfFlagOnTrimmed() throws Exception {
    final ActorPoliciesResult result = resolve(user(CALLER), Map.of("includeInactive", true));
    assertEquals(result.getView(), ActorPoliciesView.TRIMMED);
    assertTrue(result.getPolicies().stream().allMatch(p -> p.getActors() == null));
    verify(authorizer).explainUserPolicies(any(), eq(CALLER), eq(false));
  }

  @Test
  public void selfFlagOffDenied() {
    featureFlags.setActorPoliciesSelfServiceEnabled(false);
    assertDenied(user(CALLER));
  }

  @Test
  public void memberFlagOnTrimmed() throws Exception {
    when(authorizer.getActorGroupUrns(any(), eq(CALLER))).thenReturn(Set.of(GROUP));
    final ActorPoliciesResult result = resolve(group(GROUP), Map.of("includeInactive", true));
    assertEquals(result.getView(), ActorPoliciesView.TRIMMED);
    assertNull(result.getPolicies().get(0).getActors());
    verify(authorizer).explainGroupPolicies(any(), eq(GROUP), eq(false));
  }

  @Test
  public void memberFlagOffDenied() {
    featureFlags.setActorPoliciesSelfServiceEnabled(false);
    when(authorizer.getActorGroupUrns(any(), eq(CALLER))).thenReturn(Set.of(GROUP));
    assertDenied(group(GROUP));
  }

  @Test
  public void nonAdminOtherUserDenied() {
    assertDenied(user(OTHER_USER));
  }

  @Test
  public void nonAdminNonMemberGroupDenied() {
    assertDenied(group(GROUP));
  }

  @Test
  public void builtInMapsNullUrn() throws Exception {
    setAdmin(true);
    final ActorPolicyMatch builtIn =
        new ActorPolicyMatch(
            null,
            policyInfo(),
            true,
            List.of(
                new ActorPolicyMatchReason(ActorPolicyMatchReason.Type.DIRECT_USER, null, null)));
    when(authorizer.explainUserPolicies(any(), any(), anyBoolean()))
        .thenReturn(new ActorPolicies(List.of(builtIn), List.of()));
    final ActorPolicy policy = resolve(user(CALLER), null).getPolicies().get(0);
    assertNull(policy.getUrn());
    assertTrue(policy.getBuiltIn());
  }

  @Test
  public void mapsPolicyFieldsAndReasons() throws Exception {
    setAdmin(true);
    final ActorPoliciesResult result = resolve(user(OTHER_USER), null);
    final ActorPolicy policy = result.getPolicies().get(0);
    assertEquals(policy.getUrn(), POLICY.toString());
    assertEquals(policy.getName(), "Policy One");
    assertEquals(policy.getPrivileges(), List.of("VIEW_ENTITY_PAGE"));
    assertEquals(policy.getMatchedBy().get(0).getType(), PolicyMatchReasonType.GROUP);
    assertEquals(policy.getMatchedBy().get(0).getGroup().getUrn(), GROUP.toString());
    assertNull(policy.getMatchedBy().get(0).getRole());
    assertEquals(result.getOwnerPolicies(), List.of());
  }

  /**
   * Admin = every privilege check allowed (incl. MANAGE_POLICIES); otherwise every check denied.
   */
  private void setAdmin(final boolean admin) {
    context =
        admin
            ? TestUtils.getMockAllowContext(CALLER.toString())
            : TestUtils.getMockDenyContext(CALLER.toString());
    when(context.getAuthorizer()).thenReturn(chain);
  }

  private void assertDenied(final Object source) {
    final CompletionException e =
        expectThrows(CompletionException.class, () -> resolve(source, null));
    assertTrue(e.getCause() instanceof AuthorizationException);
    verify(authorizer, never()).explainUserPolicies(any(), any(), anyBoolean());
    verify(authorizer, never()).explainGroupPolicies(any(), any(), anyBoolean());
  }

  private ActorPoliciesResult resolve(final Object source, final Map<String, Object> input)
      throws Exception {
    final DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getSource()).thenReturn(source);
    when(env.getArgument("input")).thenReturn(input);
    return new ActorPoliciesResolver(featureFlags).get(env).join();
  }

  private static CorpUser user(final Urn urn) {
    final CorpUser user = new CorpUser();
    user.setUrn(urn.toString());
    return user;
  }

  private static CorpGroup group(final Urn urn) {
    final CorpGroup group = new CorpGroup();
    group.setUrn(urn.toString());
    return group;
  }

  private static ActorPolicyMatch policyMatch() {
    return new ActorPolicyMatch(
        POLICY,
        policyInfo(),
        false,
        List.of(new ActorPolicyMatchReason(ActorPolicyMatchReason.Type.GROUP, GROUP, null)));
  }

  private static DataHubPolicyInfo policyInfo() {
    return new DataHubPolicyInfo()
        .setDisplayName("Policy One")
        .setDescription("")
        .setType("PLATFORM")
        .setState("ACTIVE")
        .setPrivileges(new StringArray("VIEW_ENTITY_PAGE"))
        .setActors(new DataHubActorFilter().setGroups(new UrnArray(GROUP)))
        .setEditable(true);
  }
}
