package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import org.testng.annotations.Test;

public class PolicyEvaluationOrderTest {

  @Test
  public void legacyResourceFiltersAreAnsweredFromTheSpec() {
    assertEquals(
        PolicyEvaluationOrder.cost(policy(new DataHubResourceFilter().setType("dataset"))), 1);
    assertEquals(
        PolicyEvaluationOrder.cost(
            policy(
                new DataHubResourceFilter()
                    .setResources(new StringArray("urn:li:dataset:x"))
                    .setAllResources(false))),
        1);
    assertEquals(
        PolicyEvaluationOrder.cost(policy(new DataHubResourceFilter().setAllResources(true))), 0);
  }

  @Test
  public void ownershipTypeMatchingNeedsALookup() {
    DataHubPolicyInfo policy =
        new DataHubPolicyInfo()
            .setPrivileges(new StringArray("VIEW_ENTITY_PAGE"))
            .setActors(
                new DataHubActorFilter()
                    .setResourceOwnersTypes(
                        new UrnArray(UrnUtils.getUrn("urn:li:ownershipType:steward"))));
    assertEquals(PolicyEvaluationOrder.cost(policy), 2);
  }

  private static DataHubPolicyInfo policy(DataHubResourceFilter resources) {
    return new DataHubPolicyInfo()
        .setPrivileges(new StringArray("VIEW_ENTITY_PAGE"))
        .setActors(new DataHubActorFilter())
        .setResources(resources);
  }
}
