package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.knowledge.DocumentSearchFilterUtils.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class DocumentSearchFilterUtilsTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:test";
  private static final String TEST_GROUP_URN = "urn:li:corpGroup:testGroup";

  // ---- helpers ----

  private static Criterion findCriterion(ConjunctiveCriterion clause, String field) {
    return clause.getAnd().stream()
        .filter(c -> field.equals(c.getField()))
        .findFirst()
        .orElse(null);
  }

  private static void assertShowInGlobalContextNegated(ConjunctiveCriterion clause) {
    Criterion c = findCriterion(clause, "showInGlobalContext");
    assertNotNull(c, "showInGlobalContext criterion missing");
    assertEquals(c.getCondition(), Condition.EQUAL);
    assertEquals(c.getValues().get(0), "false");
    assertTrue(c.isNegated());
  }

  // ---- tests ----

  @Test
  public void testBuildCombinedFilterProducesFourClauses() {
    List<Criterion> baseCriteria = new ArrayList<>();
    List<String> urns = ImmutableList.of(TEST_USER_URN, TEST_GROUP_URN);

    Filter filter = DocumentSearchFilterUtils.buildCombinedFilter(baseCriteria, urns, true, false);

    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 4);
  }

  @Test
  public void testClause1_LifecyclePublished() {
    List<String> urns = ImmutableList.of(TEST_USER_URN);
    Filter filter =
        DocumentSearchFilterUtils.buildCombinedFilter(new ArrayList<>(), urns, true, false);

    // Clause 1: lifecycleStage = PUBLISHED_URN + showInGlobalContext!=false
    ConjunctiveCriterion clause = filter.getOr().get(0);
    assertEquals(clause.getAnd().size(), 2);

    Criterion lifecycle = findCriterion(clause, "lifecycleStage");
    assertNotNull(lifecycle);
    assertEquals(lifecycle.getCondition(), Condition.EQUAL);
    assertEquals(lifecycle.getValues().get(0), PUBLISHED_LIFECYCLE_STAGE_URN);

    assertShowInGlobalContextNegated(clause);
    // No owners filter required
    assertNull(findCriterion(clause, "owners"));
  }

  @Test
  public void testClause2_LifecycleNonPublishedOwned() {
    List<String> urns = ImmutableList.of(TEST_USER_URN, TEST_GROUP_URN);
    Filter filter =
        DocumentSearchFilterUtils.buildCombinedFilter(new ArrayList<>(), urns, true, false);

    // Clause 2: lifecycleStage != PUBLISHED + lifecycleStage != DRAFT + lifecycleStage EXISTS +
    // owners + showInGlobalContext!=false
    ConjunctiveCriterion clause = filter.getOr().get(1);
    assertEquals(clause.getAnd().size(), 5);

    // Collect negated lifecycleStage criteria and the EXISTS criterion
    List<Criterion> negatedLifecycleCriteria = new ArrayList<>();
    Criterion existsLifecycle = null;
    for (Criterion c : clause.getAnd()) {
      if ("lifecycleStage".equals(c.getField())) {
        if (c.getCondition() == Condition.EXISTS) {
          existsLifecycle = c;
        } else if (c.isNegated()) {
          negatedLifecycleCriteria.add(c);
        }
      }
    }

    // Must exclude both PUBLISHED and DRAFT
    assertEquals(negatedLifecycleCriteria.size(), 2);
    List<String> negatedValues =
        negatedLifecycleCriteria.stream()
            .flatMap(c -> c.getValues().stream())
            .collect(Collectors.toList());
    assertTrue(negatedValues.contains(PUBLISHED_LIFECYCLE_STAGE_URN));
    assertTrue(negatedValues.contains(DRAFT_LIFECYCLE_STAGE_URN));
    assertNotNull(existsLifecycle);

    Criterion owners = findCriterion(clause, "owners");
    assertNotNull(owners);
    assertTrue(owners.getValues().contains(TEST_USER_URN));
    assertTrue(owners.getValues().contains(TEST_GROUP_URN));

    assertShowInGlobalContextNegated(clause);
  }

  @Test
  public void testClause3_LegacyPublished() {
    List<String> urns = ImmutableList.of(TEST_USER_URN);
    Filter filter =
        DocumentSearchFilterUtils.buildCombinedFilter(new ArrayList<>(), urns, true, false);

    // Clause 3: lifecycleStage IS_NULL + state = PUBLISHED + showInGlobalContext!=false
    ConjunctiveCriterion clause = filter.getOr().get(2);
    assertEquals(clause.getAnd().size(), 3);

    Criterion lifecycle = findCriterion(clause, "lifecycleStage");
    assertNotNull(lifecycle);
    assertEquals(lifecycle.getCondition(), Condition.IS_NULL);

    Criterion state = findCriterion(clause, "state");
    assertNotNull(state);
    assertEquals(state.getCondition(), Condition.EQUAL);
    assertEquals(state.getValues().get(0), "PUBLISHED");

    assertShowInGlobalContextNegated(clause);
    assertNull(findCriterion(clause, "owners"));
  }

  @Test
  public void testClause4_LegacyUnpublishedOwned() {
    List<String> urns = ImmutableList.of(TEST_USER_URN, TEST_GROUP_URN);
    Filter filter =
        DocumentSearchFilterUtils.buildCombinedFilter(new ArrayList<>(), urns, true, false);

    // Clause 4: lifecycleStage IS_NULL + state = UNPUBLISHED + owners + showInGlobalContext!=false
    ConjunctiveCriterion clause = filter.getOr().get(3);
    assertEquals(clause.getAnd().size(), 4);

    Criterion lifecycle = findCriterion(clause, "lifecycleStage");
    assertNotNull(lifecycle);
    assertEquals(lifecycle.getCondition(), Condition.IS_NULL);

    Criterion state = findCriterion(clause, "state");
    assertNotNull(state);
    assertEquals(state.getValues().get(0), "UNPUBLISHED");

    Criterion owners = findCriterion(clause, "owners");
    assertNotNull(owners);
    assertTrue(owners.getValues().contains(TEST_USER_URN));
    assertTrue(owners.getValues().contains(TEST_GROUP_URN));

    assertShowInGlobalContextNegated(clause);
  }

  @Test
  public void testCanManageDocuments_SkipsOwnershipConstraints() {
    List<String> urns = ImmutableList.of(TEST_USER_URN);
    Filter filter =
        DocumentSearchFilterUtils.buildCombinedFilter(new ArrayList<>(), urns, true, true);

    assertEquals(filter.getOr().size(), 4);

    // Clause 2: no owners filter when canManageDocuments=true
    ConjunctiveCriterion clause2 = filter.getOr().get(1);
    assertNull(findCriterion(clause2, "owners"));

    // Clause 4: no owners filter when canManageDocuments=true
    ConjunctiveCriterion clause4 = filter.getOr().get(3);
    assertNull(findCriterion(clause4, "owners"));
  }

  @Test
  public void testWithoutShowInGlobalContext() {
    List<String> urns = ImmutableList.of(TEST_USER_URN);
    Filter filter =
        DocumentSearchFilterUtils.buildCombinedFilter(new ArrayList<>(), urns, false, false);

    assertEquals(filter.getOr().size(), 4);

    // No showInGlobalContext in any clause
    for (ConjunctiveCriterion clause : filter.getOr()) {
      assertNull(
          findCriterion(clause, "showInGlobalContext"),
          "showInGlobalContext should not be present when applyShowInGlobalContext=false");
    }
  }

  @Test
  public void testBaseCriteriaIncludedInAllClauses() {
    List<Criterion> baseCriteria = new ArrayList<>();
    baseCriteria.add(
        CriterionUtils.buildCriterion("types", Condition.EQUAL, ImmutableList.of("tutorial")));

    List<String> urns = ImmutableList.of(TEST_USER_URN);
    Filter filter = DocumentSearchFilterUtils.buildCombinedFilter(baseCriteria, urns, true, false);

    assertEquals(filter.getOr().size(), 4);

    // Every clause should include the base criteria
    for (ConjunctiveCriterion clause : filter.getOr()) {
      assertNotNull(findCriterion(clause, "types"), "Base criteria should be in every clause");
    }
  }
}
