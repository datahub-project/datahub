package com.linkedin.metadata.service.util;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.assertion.rule.AssertionAssignmentRuleFilter;
import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestSourceType;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AssertionAssignmentRuleTestBuilderTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Urn RULE_URN = UrnUtils.getUrn("urn:li:assertionAssignmentRule:test123");

  private OperationContext opContext;
  private AssertionAssignmentRuleInfo ruleInfo;

  @BeforeClass
  public void setup() {
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                CriterionUtils.buildCriterion(
                                    "platform",
                                    Condition.EQUAL,
                                    "urn:li:dataPlatform:snowflake")))));

    ruleInfo =
        new AssertionAssignmentRuleInfo()
            .setName("test")
            .setEntityFilter(new AssertionAssignmentRuleFilter().setFilter(filter));
  }

  @Test
  public void testBuildAssertionAssignmentRuleTest() {
    TestInfo testInfo =
        AssertionAssignmentRuleTestBuilder.buildAssertionAssignmentRuleTest(
            opContext, RULE_URN, ruleInfo);

    assertTrue(testInfo.getName().contains("Assertion Assignment Rule"));
    assertEquals(testInfo.getCategory(), "AssertionAssignmentRules");
    assertEquals(testInfo.getSource().getType(), TestSourceType.ASSERTION_ASSIGNMENT_RULE);
    assertEquals(testInfo.getSource().getSourceEntity(), RULE_URN);
    assertEquals(testInfo.getDefinition().getType(), TestDefinitionType.JSON);
    assertNotNull(testInfo.getCreated());
    assertNotNull(testInfo.getLastUpdated());
  }

  @Test
  public void testTestDefinitionJsonStructure() throws Exception {
    TestInfo testInfo =
        AssertionAssignmentRuleTestBuilder.buildAssertionAssignmentRuleTest(
            opContext, RULE_URN, ruleInfo);

    String json = testInfo.getDefinition().getJson();
    JsonNode root = OBJECT_MAPPER.readTree(json);

    // Verify on.types contains "dataset"
    JsonNode types = root.path("on").path("types");
    assertTrue(types.isArray());
    boolean hasDataset = false;
    for (JsonNode type : types) {
      if ("dataset".equals(type.asText())) {
        hasDataset = true;
        break;
      }
    }
    assertTrue(hasDataset, "Expected on.types to contain 'dataset'");

    // Verify passing action
    JsonNode passingAction = root.path("actions").path("passing").get(0);
    assertEquals(passingAction.path("type").asText(), "UPSERT_ASSERTION_ASSIGNMENT_RULE");
    assertTrue(passingAction.path("params").path("ruleUrn").asText().contains(RULE_URN.toString()));

    // Verify failing action
    JsonNode failingAction = root.path("actions").path("failing").get(0);
    assertEquals(failingAction.path("type").asText(), "REMOVE_ASSERTION_ASSIGNMENT_RULE");
    assertTrue(failingAction.path("params").path("ruleUrn").asText().contains(RULE_URN.toString()));
  }

  @Test
  public void testCreateTestUrn() {
    Urn testUrn = AssertionAssignmentRuleUtils.createTestUrnForAssertionAssignmentRule(RULE_URN);

    assertNotNull(testUrn);
    assertEquals(testUrn.getEntityType(), "test");
    // The URN should contain the URL-encoded rule URN
    assertTrue(testUrn.toString().contains("urn%3Ali%3AassertionAssignmentRule%3Atest123"));
  }
}
