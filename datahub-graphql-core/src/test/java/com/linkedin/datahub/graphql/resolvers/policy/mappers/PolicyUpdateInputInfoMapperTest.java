package com.linkedin.datahub.graphql.resolvers.policy.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.datahub.graphql.generated.ActorFilterInput;
import com.linkedin.datahub.graphql.generated.PolicyMatchCondition;
import com.linkedin.datahub.graphql.generated.PolicyMatchCriterionInput;
import com.linkedin.datahub.graphql.generated.PolicyMatchFilterInput;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.datahub.graphql.generated.PolicyUpdateInput;
import com.linkedin.datahub.graphql.generated.ResourceFilterInput;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.List;
import org.testng.annotations.Test;

public class PolicyUpdateInputInfoMapperTest {

  @Test
  public void testMapBasicPolicyInput() {
    PolicyUpdateInput input = new PolicyUpdateInput();
    input.setName("Test Policy");
    input.setDescription("Test Description");
    input.setType(PolicyType.METADATA);
    input.setState(PolicyState.ACTIVE);
    input.setPrivileges(List.of("EDIT_ENTITY"));

    ActorFilterInput actorFilter = new ActorFilterInput();
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(false);
    actorFilter.setResourceOwners(false);
    input.setActors(actorFilter);

    DataHubPolicyInfo result = PolicyUpdateInputInfoMapper.map(null, input);

    assertNotNull(result);
    assertEquals(result.getDisplayName(), "Test Policy");
    assertEquals(result.getDescription(), "Test Description");
    assertEquals(result.getType(), "METADATA");
    assertEquals(result.getState(), "ACTIVE");
    assertEquals(result.getPrivileges().size(), 1);
    assertEquals(result.getPrivileges().get(0), "EDIT_ENTITY");
    assertNotNull(result.getActors());
    assertEquals(result.getActors().isAllUsers(), true);
  }

  @Test
  public void testMapPolicyWithValidFilterFieldType() {
    PolicyUpdateInput input = createBasicPolicyInput();

    ResourceFilterInput resourceFilter = new ResourceFilterInput();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("DATASET");

    PolicyMatchFilterInput filter = new PolicyMatchFilterInput();
    PolicyMatchCriterionInput criterion = new PolicyMatchCriterionInput();
    criterion.setField("TYPE");
    criterion.setValues(List.of("dataset"));
    criterion.setCondition(PolicyMatchCondition.EQUALS);
    filter.setCriteria(List.of(criterion));
    resourceFilter.setFilter(filter);

    input.setResources(resourceFilter);

    DataHubPolicyInfo result = PolicyUpdateInputInfoMapper.map(null, input);

    assertNotNull(result);
    assertNotNull(result.getResources());
    assertNotNull(result.getResources().getFilter());
    assertEquals(result.getResources().getFilter().getCriteria().size(), 1);
    assertEquals(result.getResources().getFilter().getCriteria().get(0).getField(), "TYPE");
  }

  @Test
  public void testMapPolicyWithInvalidFilterFieldType() {
    PolicyUpdateInput input = createBasicPolicyInput();

    ResourceFilterInput resourceFilter = new ResourceFilterInput();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("DATASET");

    PolicyMatchFilterInput filter = new PolicyMatchFilterInput();
    PolicyMatchCriterionInput criterion = new PolicyMatchCriterionInput();
    criterion.setField("INVALID_FIELD_TYPE");
    criterion.setValues(List.of("somevalue"));
    criterion.setCondition(PolicyMatchCondition.EQUALS);
    filter.setCriteria(List.of(criterion));
    resourceFilter.setFilter(filter);

    input.setResources(resourceFilter);

    assertThrows(
        IllegalArgumentException.class, () -> PolicyUpdateInputInfoMapper.map(null, input));
  }

  @Test
  public void testMapPolicyWithValidPrivilegeConstraints() {
    PolicyUpdateInput input = createBasicPolicyInput();

    ResourceFilterInput resourceFilter = new ResourceFilterInput();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("DATASET");

    PolicyMatchFilterInput privilegeConstraints = new PolicyMatchFilterInput();
    PolicyMatchCriterionInput criterion = new PolicyMatchCriterionInput();
    criterion.setField("DOMAIN");
    criterion.setValues(List.of("urn:li:domain:engineering"));
    criterion.setCondition(PolicyMatchCondition.EQUALS);
    privilegeConstraints.setCriteria(List.of(criterion));
    resourceFilter.setPrivilegeConstraints(privilegeConstraints);

    input.setResources(resourceFilter);

    DataHubPolicyInfo result = PolicyUpdateInputInfoMapper.map(null, input);

    assertNotNull(result);
    assertNotNull(result.getResources());
    assertNotNull(result.getResources().getPrivilegeConstraints());
    assertEquals(result.getResources().getPrivilegeConstraints().getCriteria().size(), 1);
    assertEquals(
        result.getResources().getPrivilegeConstraints().getCriteria().get(0).getField(), "DOMAIN");
  }

  @Test
  public void testMapPolicyWithInvalidPrivilegeConstraints() {
    PolicyUpdateInput input = createBasicPolicyInput();

    ResourceFilterInput resourceFilter = new ResourceFilterInput();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("DATASET");

    PolicyMatchFilterInput privilegeConstraints = new PolicyMatchFilterInput();
    PolicyMatchCriterionInput criterion = new PolicyMatchCriterionInput();
    criterion.setField("INVALID_CONSTRAINT");
    criterion.setValues(List.of("somevalue"));
    criterion.setCondition(PolicyMatchCondition.EQUALS);
    privilegeConstraints.setCriteria(List.of(criterion));
    resourceFilter.setPrivilegeConstraints(privilegeConstraints);

    input.setResources(resourceFilter);

    assertThrows(
        IllegalArgumentException.class, () -> PolicyUpdateInputInfoMapper.map(null, input));
  }

  @Test
  public void testMapPolicyWithMultipleCriteria() {
    PolicyUpdateInput input = createBasicPolicyInput();

    ResourceFilterInput resourceFilter = new ResourceFilterInput();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("DATASET");

    PolicyMatchFilterInput filter = new PolicyMatchFilterInput();

    PolicyMatchCriterionInput criterion1 = new PolicyMatchCriterionInput();
    criterion1.setField("TYPE");
    criterion1.setValues(List.of("dataset"));
    criterion1.setCondition(PolicyMatchCondition.EQUALS);

    PolicyMatchCriterionInput criterion2 = new PolicyMatchCriterionInput();
    criterion2.setField("DOMAIN");
    criterion2.setValues(List.of("urn:li:domain:engineering"));
    criterion2.setCondition(PolicyMatchCondition.EQUALS);

    filter.setCriteria(List.of(criterion1, criterion2));
    resourceFilter.setFilter(filter);

    input.setResources(resourceFilter);

    DataHubPolicyInfo result = PolicyUpdateInputInfoMapper.map(null, input);

    assertNotNull(result);
    assertNotNull(result.getResources());
    assertNotNull(result.getResources().getFilter());
    assertEquals(result.getResources().getFilter().getCriteria().size(), 2);
    assertEquals(result.getResources().getFilter().getCriteria().get(0).getField(), "TYPE");
    assertEquals(result.getResources().getFilter().getCriteria().get(1).getField(), "DOMAIN");
  }

  @Test
  public void testMapPolicyWithDeprecatedFieldTypes() {
    PolicyUpdateInput input = createBasicPolicyInput();

    ResourceFilterInput resourceFilter = new ResourceFilterInput();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("DATASET");

    PolicyMatchFilterInput filter = new PolicyMatchFilterInput();
    PolicyMatchCriterionInput criterion = new PolicyMatchCriterionInput();
    criterion.setField("RESOURCE_URN");
    criterion.setValues(List.of("urn:li:dataset:test"));
    criterion.setCondition(PolicyMatchCondition.EQUALS);
    filter.setCriteria(List.of(criterion));
    resourceFilter.setFilter(filter);

    input.setResources(resourceFilter);

    DataHubPolicyInfo result = PolicyUpdateInputInfoMapper.map(null, input);

    assertNotNull(result);
    assertNotNull(result.getResources());
    assertNotNull(result.getResources().getFilter());
    assertEquals(result.getResources().getFilter().getCriteria().size(), 1);
    assertEquals(result.getResources().getFilter().getCriteria().get(0).getField(), "RESOURCE_URN");
  }

  private PolicyUpdateInput createBasicPolicyInput() {
    PolicyUpdateInput input = new PolicyUpdateInput();
    input.setName("Test Policy");
    input.setDescription("Test Description");
    input.setType(PolicyType.METADATA);
    input.setState(PolicyState.ACTIVE);
    input.setPrivileges(List.of("EDIT_ENTITY"));

    ActorFilterInput actorFilter = new ActorFilterInput();
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(false);
    actorFilter.setResourceOwners(false);
    input.setActors(actorFilter);

    return input;
  }
}
