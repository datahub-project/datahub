package com.linkedin.datahub.graphql.resolvers.policy.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.generated.ActorFilterInput;
import com.linkedin.datahub.graphql.generated.PolicyMatchCondition;
import com.linkedin.datahub.graphql.generated.PolicyMatchCriterionInput;
import com.linkedin.datahub.graphql.generated.PolicyMatchFilterInput;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.datahub.graphql.generated.PolicyUpdateInput;
import com.linkedin.datahub.graphql.generated.ResourceFilterInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyCriterionValueInput;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.PolicyMatchCriterion;
import java.util.Arrays;
import org.testng.annotations.Test;

public class PolicyUpdateInputInfoMapperTest {

  private static final PolicyUpdateInputInfoMapper mapper = PolicyUpdateInputInfoMapper.INSTANCE;

  private static PolicyUpdateInput createBasicPolicyInput(String policyName) {
    PolicyUpdateInput input = new PolicyUpdateInput();
    input.setName(policyName);
    input.setType(PolicyType.METADATA);
    input.setState(PolicyState.ACTIVE);
    input.setPrivileges(Arrays.asList("EDIT_ENTITY_TAGS"));
    input.setActors(new ActorFilterInput());
    input.getActors().setAllUsers(true);
    return input;
  }

  private static void applyResourceFilter(
      PolicyUpdateInput input, PolicyMatchCriterionInput criterion) {
    PolicyMatchFilterInput filter = new PolicyMatchFilterInput();
    filter.setCriteria(Arrays.asList(criterion));

    ResourceFilterInput resources = new ResourceFilterInput();
    resources.setAllResources(true);
    resources.setFilter(filter);
    input.setResources(resources);
  }

  private static void applyResourceFilter(
      PolicyUpdateInput input, PolicyMatchCriterionInput... criteria) {
    PolicyMatchFilterInput filter = new PolicyMatchFilterInput();
    filter.setCriteria(Arrays.asList(criteria));

    ResourceFilterInput resources = new ResourceFilterInput();
    resources.setAllResources(true);
    resources.setFilter(filter);
    input.setResources(resources);
  }

  @Test
  public void testMapBasicPolicyFields() {
    PolicyUpdateInput input = createBasicPolicyInput("Test Policy");

    DataHubPolicyInfo result = mapper.map(null, input);

    assertEquals("Test Policy", result.getDisplayName());
    assertEquals("METADATA", result.getType());
    assertEquals("ACTIVE", result.getState());
    assertEquals(1, result.getPrivileges().size());
    assertTrue(result.getActors().isAllUsers());
  }

  @Test
  public void testMapTraditionalFieldCriteria() {
    PolicyUpdateInput input = createBasicPolicyInput("Tag Policy");

    // Traditional TAG field criterion
    PolicyMatchCriterionInput tagCriterion = new PolicyMatchCriterionInput();
    tagCriterion.setField("TAG");
    tagCriterion.setValues(Arrays.asList("urn:li:tag:PII"));
    tagCriterion.setCondition(PolicyMatchCondition.EQUALS);

    applyResourceFilter(input, tagCriterion);

    DataHubPolicyInfo result = mapper.map(null, input);

    assertNotNull(result.getResources().getFilter());
    assertEquals(1, result.getResources().getFilter().getCriteria().size());
    PolicyMatchCriterion criterion = result.getResources().getFilter().getCriteria().get(0);
    assertEquals("TAG", criterion.getField());
    assertEquals(1, criterion.getValues().size());
    assertEquals("urn:li:tag:PII", criterion.getValues().get(0));
    assertEquals("EQUALS", criterion.getCondition().toString());
  }

  @Test
  public void testMapStructuredPropertyCriteria() {
    PolicyUpdateInput input = createBasicPolicyInput("Structured Property Policy");

    // Structured property criterion
    StructuredPropertyCriterionValueInput propValue = new StructuredPropertyCriterionValueInput();
    propValue.setPropertyUrn("urn:li:structuredProperty:data_classification");
    propValue.setValues(Arrays.asList("high", "sensitive"));

    PolicyMatchCriterionInput structuredPropCriterion = new PolicyMatchCriterionInput();
    structuredPropCriterion.setField("STRUCTURED_PROPERTY");
    structuredPropCriterion.setValues(Arrays.asList()); // Empty for structured properties
    structuredPropCriterion.setStructuredPropertyValues(Arrays.asList(propValue));
    structuredPropCriterion.setCondition(PolicyMatchCondition.EQUALS);

    applyResourceFilter(input, structuredPropCriterion);

    DataHubPolicyInfo result = mapper.map(null, input);

    assertNotNull(result.getResources().getFilter());
    assertEquals(1, result.getResources().getFilter().getCriteria().size());
    PolicyMatchCriterion criterion = result.getResources().getFilter().getCriteria().get(0);

    // Verify traditional fields
    assertEquals("STRUCTURED_PROPERTY", criterion.getField());
    assertEquals("EQUALS", criterion.getCondition().toString());

    // Verify structured property values are mapped
    assertNotNull(criterion.getStructuredPropertyValues());
    assertEquals(1, criterion.getStructuredPropertyValues().size());
    assertEquals(
        "urn:li:structuredProperty:data_classification",
        criterion.getStructuredPropertyValues().get(0).getPropertyUrn());
    assertEquals(2, criterion.getStructuredPropertyValues().get(0).getValues().size());
    assertEquals("high", criterion.getStructuredPropertyValues().get(0).getValues().get(0));
    assertEquals("sensitive", criterion.getStructuredPropertyValues().get(0).getValues().get(1));
  }

  @Test
  public void testMapMultipleStructuredProperties() {
    PolicyUpdateInput input = createBasicPolicyInput("Multi Structured Property Policy");

    // First structured property
    StructuredPropertyCriterionValueInput propValue1 = new StructuredPropertyCriterionValueInput();
    propValue1.setPropertyUrn("urn:li:structuredProperty:dept");
    propValue1.setValues(Arrays.asList("sales", "eng"));

    // Second structured property
    StructuredPropertyCriterionValueInput propValue2 = new StructuredPropertyCriterionValueInput();
    propValue2.setPropertyUrn("urn:li:structuredProperty:data_classification");
    propValue2.setValues(Arrays.asList("high"));

    PolicyMatchCriterionInput structuredPropCriterion = new PolicyMatchCriterionInput();
    structuredPropCriterion.setField("STRUCTURED_PROPERTY");
    structuredPropCriterion.setValues(Arrays.asList());
    structuredPropCriterion.setStructuredPropertyValues(Arrays.asList(propValue1, propValue2));
    structuredPropCriterion.setCondition(PolicyMatchCondition.EQUALS);

    applyResourceFilter(input, structuredPropCriterion);

    DataHubPolicyInfo result = mapper.map(null, input);

    PolicyMatchCriterion criterion = result.getResources().getFilter().getCriteria().get(0);
    assertNotNull(criterion.getStructuredPropertyValues());
    assertEquals(2, criterion.getStructuredPropertyValues().size());

    // Verify first property
    assertEquals(
        "urn:li:structuredProperty:dept",
        criterion.getStructuredPropertyValues().get(0).getPropertyUrn());
    assertEquals(2, criterion.getStructuredPropertyValues().get(0).getValues().size());

    // Verify second property
    assertEquals(
        "urn:li:structuredProperty:data_classification",
        criterion.getStructuredPropertyValues().get(1).getPropertyUrn());
    assertEquals(1, criterion.getStructuredPropertyValues().get(1).getValues().size());
  }

  @Test
  public void testMapMixedCriteria() {
    PolicyUpdateInput input = createBasicPolicyInput("Mixed Criteria Policy");

    // TAG criterion
    PolicyMatchCriterionInput tagCriterion = new PolicyMatchCriterionInput();
    tagCriterion.setField("TAG");
    tagCriterion.setValues(Arrays.asList("urn:li:tag:PII"));
    tagCriterion.setCondition(PolicyMatchCondition.EQUALS);

    // Structured property criterion
    StructuredPropertyCriterionValueInput propValue = new StructuredPropertyCriterionValueInput();
    propValue.setPropertyUrn("urn:li:structuredProperty:dept");
    propValue.setValues(Arrays.asList("sales"));

    PolicyMatchCriterionInput structuredPropCriterion = new PolicyMatchCriterionInput();
    structuredPropCriterion.setField("STRUCTURED_PROPERTY");
    structuredPropCriterion.setValues(Arrays.asList());
    structuredPropCriterion.setStructuredPropertyValues(Arrays.asList(propValue));
    structuredPropCriterion.setCondition(PolicyMatchCondition.EQUALS);

    applyResourceFilter(input, tagCriterion, structuredPropCriterion);

    DataHubPolicyInfo result = mapper.map(null, input);

    assertEquals(2, result.getResources().getFilter().getCriteria().size());

    // Verify TAG criterion
    PolicyMatchCriterion tagCrit = result.getResources().getFilter().getCriteria().get(0);
    assertEquals("TAG", tagCrit.getField());
    assertEquals(1, tagCrit.getValues().size());

    // Verify Structured Property criterion
    PolicyMatchCriterion structCrit = result.getResources().getFilter().getCriteria().get(1);
    assertEquals("STRUCTURED_PROPERTY", structCrit.getField());
    assertNotNull(structCrit.getStructuredPropertyValues());
    assertEquals(1, structCrit.getStructuredPropertyValues().size());
  }

  @Test
  public void testMapStructuredPropertyWithStartsWithCondition() {
    PolicyUpdateInput input = createBasicPolicyInput("Starts With Policy");

    StructuredPropertyCriterionValueInput propValue = new StructuredPropertyCriterionValueInput();
    propValue.setPropertyUrn("urn:li:structuredProperty:name");
    propValue.setValues(Arrays.asList("test_"));

    PolicyMatchCriterionInput structuredPropCriterion = new PolicyMatchCriterionInput();
    structuredPropCriterion.setField("STRUCTURED_PROPERTY");
    structuredPropCriterion.setValues(Arrays.asList());
    structuredPropCriterion.setStructuredPropertyValues(Arrays.asList(propValue));
    structuredPropCriterion.setCondition(PolicyMatchCondition.STARTS_WITH);

    applyResourceFilter(input, structuredPropCriterion);

    DataHubPolicyInfo result = mapper.map(null, input);

    PolicyMatchCriterion criterion = result.getResources().getFilter().getCriteria().get(0);
    assertEquals("STARTS_WITH", criterion.getCondition().toString());
    assertNotNull(criterion.getStructuredPropertyValues());
  }

  @Test
  public void testMapStructuredPropertyWithNotEqualsCondition() {
    PolicyUpdateInput input = createBasicPolicyInput("Not Equals Policy");

    StructuredPropertyCriterionValueInput propValue = new StructuredPropertyCriterionValueInput();
    propValue.setPropertyUrn("urn:li:structuredProperty:status");
    propValue.setValues(Arrays.asList("inactive"));

    PolicyMatchCriterionInput structuredPropCriterion = new PolicyMatchCriterionInput();
    structuredPropCriterion.setField("STRUCTURED_PROPERTY");
    structuredPropCriterion.setValues(Arrays.asList());
    structuredPropCriterion.setStructuredPropertyValues(Arrays.asList(propValue));
    structuredPropCriterion.setCondition(PolicyMatchCondition.NOT_EQUALS);

    applyResourceFilter(input, structuredPropCriterion);

    DataHubPolicyInfo result = mapper.map(null, input);

    PolicyMatchCriterion criterion = result.getResources().getFilter().getCriteria().get(0);
    assertEquals("NOT_EQUALS", criterion.getCondition().toString());
    assertNotNull(criterion.getStructuredPropertyValues());
  }

  @Test
  public void testMapEmptyStructuredPropertyValues() {
    PolicyUpdateInput input = createBasicPolicyInput("Empty Structured Property Policy");

    PolicyMatchCriterionInput structuredPropCriterion = new PolicyMatchCriterionInput();
    structuredPropCriterion.setField("STRUCTURED_PROPERTY");
    structuredPropCriterion.setValues(Arrays.asList());
    structuredPropCriterion.setStructuredPropertyValues(Arrays.asList());
    structuredPropCriterion.setCondition(PolicyMatchCondition.EQUALS);

    applyResourceFilter(input, structuredPropCriterion);

    DataHubPolicyInfo result = mapper.map(null, input);

    PolicyMatchCriterion criterion = result.getResources().getFilter().getCriteria().get(0);
    assertEquals("STRUCTURED_PROPERTY", criterion.getField());
    assertTrue(
        criterion.getStructuredPropertyValues() == null
            || criterion.getStructuredPropertyValues().isEmpty());
  }
}
