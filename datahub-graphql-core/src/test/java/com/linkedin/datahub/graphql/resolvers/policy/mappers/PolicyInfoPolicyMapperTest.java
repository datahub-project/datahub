package com.linkedin.datahub.graphql.resolvers.policy.mappers;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.DataHubPolicy;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.generated.PolicyMatchCondition;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.datahub.graphql.types.policy.DataHubPolicyMapper;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.UnknownDataHandlings;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchFilter;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Guards against a single malformed policy taking down the entire {@code listPolicies} page.
 *
 * <p>{@link DataHubPolicyInfo#getType()} and {@link DataHubPolicyInfo#getState()} are free-form
 * strings in the metadata model, so a policy created via ingestion / the API can carry a value that
 * is not a member of the GraphQL {@link PolicyType} / {@link PolicyState} enums. An unguarded
 * {@code Enum.valueOf} throws {@link IllegalArgumentException}, which fails the whole request and
 * produces the UI's "Failed to load policies! An unexpected error occurred." on any page containing
 * such a policy.
 */
public class PolicyInfoPolicyMapperTest {

  private static DataHubPolicyInfo policyInfo(final String type, final String state) {
    final DataHubPolicyInfo info = new DataHubPolicyInfo();
    info.setDisplayName("My Policy");
    info.setDescription("desc");
    info.setType(type);
    info.setState(state);
    info.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    info.setActors(new DataHubActorFilter().setAllUsers(true).setAllGroups(false));
    info.setEditable(true);
    return info;
  }

  @Test
  public void testMapValidPolicy() {
    final Policy result = PolicyInfoPolicyMapper.map(null, policyInfo("METADATA", "ACTIVE"));
    assertEquals(result.getType(), PolicyType.METADATA);
    assertEquals(result.getState(), PolicyState.ACTIVE);
    assertEquals(result.getName(), "My Policy");
  }

  @Test
  public void testUnknownStateFallsBackToInactive() {
    // A state value not present in the GraphQL enum (e.g. ingested/backdoored data) must not throw.
    final Policy result = PolicyInfoPolicyMapper.map(null, policyInfo("METADATA", "ENABLED"));
    assertEquals(result.getState(), PolicyState.INACTIVE);
    // The rest of the policy still maps so it stays visible/manageable in the UI.
    assertEquals(result.getName(), "My Policy");
    assertEquals(result.getType(), PolicyType.METADATA);
  }

  @Test
  public void testUnknownTypeFallsBackToMetadata() {
    final Policy result = PolicyInfoPolicyMapper.map(null, policyInfo("SUPERUSER", "ACTIVE"));
    assertEquals(result.getType(), PolicyType.METADATA);
    assertEquals(result.getState(), PolicyState.ACTIVE);
  }

  private static DataHubPolicyInfo policyWithCondition(final String condition) {
    final DataMap criterion = new DataMap();
    criterion.put("field", "TYPE");
    criterion.put("values", new DataList(List.of("dataset")));
    criterion.put("condition", condition);
    final DataHubPolicyInfo info = policyInfo("METADATA", "ACTIVE");
    info.setResources(
        new DataHubResourceFilter()
            .setFilter(
                new PolicyMatchFilter(
                    new DataMap(Map.of("criteria", new DataList(List.of(criterion)))))));
    return info;
  }

  @Test
  public void testPolicyWithMatchConditionFromNewerVersionIsLeftOut() {
    // A guessed condition would be saved back by the policy editor and change what the policy
    // matches, so the policy is left out rather than shown with a different condition.
    assertTrue(
        PolicyInfoPolicyMapper.isUnrepresentable(
            policyWithCondition("CONDITION_FROM_NEWER_BUILD"), "urn:li:dataHubPolicy:x"));

    final DataHubPolicyInfo known = policyWithCondition("STARTS_WITH");
    assertFalse(PolicyInfoPolicyMapper.isUnrepresentable(known, "urn:li:dataHubPolicy:y"));
    assertEquals(
        PolicyInfoPolicyMapper.map(null, known)
            .getResources()
            .getFilter()
            .getCriteria()
            .get(0)
            .getCondition(),
        PolicyMatchCondition.STARTS_WITH);
  }

  @Test
  public void testEntityLoadOfPolicyWithMatchConditionFromNewerVersionKeepsThePolicy() {
    // Read-only path (entity, search): a null policy would null the field or list that loaded it,
    // so only the unknown condition is unset. The editor's list still leaves the policy out.
    final Urn urn = UrnUtils.getUrn("urn:li:dataHubPolicy:x");
    final EntityResponse response =
        new EntityResponse()
            .setUrn(urn)
            .setEntityName("dataHubPolicy")
            .setAspects(
                new EnvelopedAspectMap(
                    Map.of(
                        DATAHUB_POLICY_INFO_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setValue(
                                new Aspect(
                                    policyWithCondition("CONDITION_FROM_NEWER_BUILD").data())))));

    final DataHubPolicy policy = DataHubPolicyMapper.map(null, response);

    assertEquals(policy.getUrn(), urn.toString());
    assertNull(policy.getResources().getFilter().getCriteria().get(0).getCondition());
  }

  @Test
  public void testStrictHandlingFailsOnMatchConditionFromNewerVersion() {
    UnknownDataHandlings.use(StrictHandling.INSTANCE);
    try {
      final DataHubPolicyInfo info = policyWithCondition("CONDITION_FROM_NEWER_BUILD");
      assertFalse(PolicyInfoPolicyMapper.isUnrepresentable(info, "urn:li:dataHubPolicy:x"));
      expectThrows(IllegalArgumentException.class, () -> PolicyInfoPolicyMapper.map(null, info));
    } finally {
      UnknownDataHandlings.reset();
    }
  }
}
