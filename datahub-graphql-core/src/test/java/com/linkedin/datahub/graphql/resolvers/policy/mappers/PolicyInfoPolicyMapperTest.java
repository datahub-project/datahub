package com.linkedin.datahub.graphql.resolvers.policy.mappers;

import static org.testng.Assert.assertEquals;

import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
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
}
