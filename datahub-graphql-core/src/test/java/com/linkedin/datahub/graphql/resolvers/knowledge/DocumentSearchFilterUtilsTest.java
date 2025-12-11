package com.linkedin.datahub.graphql.resolvers.knowledge;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class DocumentSearchFilterUtilsTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:test";
  private static final String TEST_GROUP_URN = "urn:li:corpGroup:testGroup";

  @Test
  public void testBuildCombinedFilterWithEmptyBaseCriteria() {
    List<Criterion> baseCriteria = new ArrayList<>();
    List<String> userAndGroupUrns = ImmutableList.of(TEST_USER_URN, TEST_GROUP_URN);

    Filter filter = DocumentSearchFilterUtils.buildCombinedFilter(baseCriteria, userAndGroupUrns);

    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 2); // PUBLISHED OR UNPUBLISHED owned

    // Check first clause: PUBLISHED AND NOT-DRAFT AND showInGlobalContext=true
    ConjunctiveCriterion publishedClause = filter.getOr().get(0);
    assertNotNull(publishedClause.getAnd());
    assertEquals(
        publishedClause.getAnd().size(), 3); // state + draftOf IS_NULL + showInGlobalContext
    Criterion stateCriterion = publishedClause.getAnd().get(0);
    assertEquals(stateCriterion.getField(), "state");
    assertEquals(stateCriterion.getCondition(), Condition.EQUAL);
    assertEquals(stateCriterion.getValues().get(0), "PUBLISHED");
    // Verify draftOf IS_NULL filter
    Criterion draftOfCriterion = publishedClause.getAnd().get(1);
    assertEquals(draftOfCriterion.getField(), "draftOf");
    assertEquals(draftOfCriterion.getCondition(), Condition.IS_NULL);
    // Verify showInGlobalContext filter
    Criterion showInGlobalContextCriterion = publishedClause.getAnd().get(2);
    assertEquals(showInGlobalContextCriterion.getField(), "showInGlobalContext");
    assertEquals(showInGlobalContextCriterion.getCondition(), Condition.EQUAL);
    assertEquals(showInGlobalContextCriterion.getValues().get(0), "true");

    // Check second clause: UNPUBLISHED AND owned AND NOT-DRAFT AND showInGlobalContext=true
    ConjunctiveCriterion unpublishedClause = filter.getOr().get(1);
    assertNotNull(unpublishedClause.getAnd());
    assertEquals(
        unpublishedClause.getAnd().size(),
        4); // State + owners + draftOf IS_NULL + showInGlobalContext
    Criterion unpublishedStateCriterion = unpublishedClause.getAnd().get(0);
    assertEquals(unpublishedStateCriterion.getField(), "state");
    assertEquals(unpublishedStateCriterion.getCondition(), Condition.EQUAL);
    assertEquals(unpublishedStateCriterion.getValues().get(0), "UNPUBLISHED");
    Criterion ownersCriterion = unpublishedClause.getAnd().get(1);
    assertEquals(ownersCriterion.getField(), "owners");
    assertEquals(ownersCriterion.getCondition(), Condition.EQUAL);
    assertTrue(ownersCriterion.getValues().contains(TEST_USER_URN));
    assertTrue(ownersCriterion.getValues().contains(TEST_GROUP_URN));
    // Verify draftOf IS_NULL filter in unpublished clause
    Criterion unpublishedDraftOfCriterion = unpublishedClause.getAnd().get(2);
    assertEquals(unpublishedDraftOfCriterion.getField(), "draftOf");
    assertEquals(unpublishedDraftOfCriterion.getCondition(), Condition.IS_NULL);
    // Verify showInGlobalContext filter in unpublished clause
    Criterion unpublishedShowInGlobalContextCriterion = unpublishedClause.getAnd().get(3);
    assertEquals(unpublishedShowInGlobalContextCriterion.getField(), "showInGlobalContext");
    assertEquals(unpublishedShowInGlobalContextCriterion.getCondition(), Condition.EQUAL);
    assertEquals(unpublishedShowInGlobalContextCriterion.getValues().get(0), "true");
  }

  @Test
  public void testBuildCombinedFilterWithBaseCriteria() {
    List<Criterion> baseCriteria = new ArrayList<>();
    baseCriteria.add(
        CriterionUtils.buildCriterion("types", Condition.EQUAL, ImmutableList.of("tutorial")));
    baseCriteria.add(
        CriterionUtils.buildCriterion(
            "domains", Condition.EQUAL, ImmutableList.of("urn:li:domain:test")));

    List<String> userAndGroupUrns = ImmutableList.of(TEST_USER_URN);

    Filter filter = DocumentSearchFilterUtils.buildCombinedFilter(baseCriteria, userAndGroupUrns);

    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 2);

    // Check first clause: base criteria AND PUBLISHED AND NOT-DRAFT AND showInGlobalContext=true
    ConjunctiveCriterion publishedClause = filter.getOr().get(0);
    assertNotNull(publishedClause.getAnd());
    assertEquals(
        publishedClause.getAnd().size(),
        5); // types + domains + state + draftOf IS_NULL + showInGlobalContext

    // Check second clause: base criteria AND UNPUBLISHED AND owned AND NOT-DRAFT AND
    // showInGlobalContext=true
    ConjunctiveCriterion unpublishedClause = filter.getOr().get(1);
    assertNotNull(unpublishedClause.getAnd());
    assertEquals(
        unpublishedClause.getAnd().size(),
        6); // types + domains + state + owners + draftOf IS_NULL + showInGlobalContext

    // Verify base criteria are in both clauses
    boolean foundTypesInPublished = false;
    boolean foundDomainsInPublished = false;
    boolean foundTypesInUnpublished = false;
    boolean foundDomainsInUnpublished = false;
    boolean foundDraftOfInPublished = false;
    boolean foundDraftOfInUnpublished = false;
    boolean foundShowInGlobalContextInPublished = false;
    boolean foundShowInGlobalContextInUnpublished = false;

    for (Criterion criterion : publishedClause.getAnd()) {
      if ("types".equals(criterion.getField())) {
        foundTypesInPublished = true;
      }
      if ("domains".equals(criterion.getField())) {
        foundDomainsInPublished = true;
      }
      if ("draftOf".equals(criterion.getField()) && criterion.getCondition() == Condition.IS_NULL) {
        foundDraftOfInPublished = true;
      }
      if ("showInGlobalContext".equals(criterion.getField())) {
        foundShowInGlobalContextInPublished = true;
      }
    }

    for (Criterion criterion : unpublishedClause.getAnd()) {
      if ("types".equals(criterion.getField())) {
        foundTypesInUnpublished = true;
      }
      if ("domains".equals(criterion.getField())) {
        foundDomainsInUnpublished = true;
      }
      if ("draftOf".equals(criterion.getField()) && criterion.getCondition() == Condition.IS_NULL) {
        foundDraftOfInUnpublished = true;
      }
      if ("showInGlobalContext".equals(criterion.getField())) {
        foundShowInGlobalContextInUnpublished = true;
      }
    }

    assertTrue(foundTypesInPublished, "Types filter should be in published clause");
    assertTrue(foundDomainsInPublished, "Domains filter should be in published clause");
    assertTrue(foundDraftOfInPublished, "DraftOf IS_NULL filter should be in published clause");
    assertTrue(
        foundShowInGlobalContextInPublished,
        "showInGlobalContext filter should be in published clause");
    assertTrue(foundTypesInUnpublished, "Types filter should be in unpublished clause");
    assertTrue(foundDomainsInUnpublished, "Domains filter should be in unpublished clause");
    assertTrue(foundDraftOfInUnpublished, "DraftOf IS_NULL filter should be in unpublished clause");
    assertTrue(
        foundShowInGlobalContextInUnpublished,
        "showInGlobalContext filter should be in unpublished clause");
  }

  @Test
  public void testBuildCombinedFilterWithSingleUser() {
    List<Criterion> baseCriteria = new ArrayList<>();
    List<String> userAndGroupUrns = ImmutableList.of(TEST_USER_URN);

    Filter filter = DocumentSearchFilterUtils.buildCombinedFilter(baseCriteria, userAndGroupUrns);

    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 2);

    // Check owners filter in unpublished clause (state, owners, draftOf, showInGlobalContext)
    ConjunctiveCriterion unpublishedClause = filter.getOr().get(1);
    assertEquals(
        unpublishedClause.getAnd().size(),
        4); // state + owners + draftOf IS_NULL + showInGlobalContext
    Criterion ownersCriterion = unpublishedClause.getAnd().get(1);
    assertEquals(ownersCriterion.getField(), "owners");
    assertEquals(ownersCriterion.getValues().size(), 1);
    assertEquals(ownersCriterion.getValues().get(0), TEST_USER_URN);
  }

  @Test
  public void testBuildCombinedFilterWithMultipleGroups() {
    List<Criterion> baseCriteria = new ArrayList<>();
    List<String> userAndGroupUrns =
        ImmutableList.of(TEST_USER_URN, TEST_GROUP_URN, "urn:li:corpGroup:anotherGroup");

    Filter filter = DocumentSearchFilterUtils.buildCombinedFilter(baseCriteria, userAndGroupUrns);

    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 2);

    // Check owners filter includes all URNs (state, owners, draftOf, showInGlobalContext)
    ConjunctiveCriterion unpublishedClause = filter.getOr().get(1);
    assertEquals(
        unpublishedClause.getAnd().size(),
        4); // state + owners + draftOf IS_NULL + showInGlobalContext
    Criterion ownersCriterion = unpublishedClause.getAnd().get(1);
    assertEquals(ownersCriterion.getField(), "owners");
    assertEquals(ownersCriterion.getValues().size(), 3);
    assertTrue(ownersCriterion.getValues().contains(TEST_USER_URN));
    assertTrue(ownersCriterion.getValues().contains(TEST_GROUP_URN));
    assertTrue(ownersCriterion.getValues().contains("urn:li:corpGroup:anotherGroup"));
  }

  @Test
  public void testBuildCombinedFilterStructure() {
    // Verify the filter structure: (base AND PUBLISHED AND NOT-DRAFT AND showInGlobalContext=true)
    // OR (base AND UNPUBLISHED AND owners AND NOT-DRAFT AND showInGlobalContext=true)
    List<Criterion> baseCriteria = new ArrayList<>();
    baseCriteria.add(
        CriterionUtils.buildCriterion(
            "relatedAssets", Condition.EQUAL, ImmutableList.of("urn:li:dataset:test")));

    List<String> userAndGroupUrns = ImmutableList.of(TEST_USER_URN);

    Filter filter = DocumentSearchFilterUtils.buildCombinedFilter(baseCriteria, userAndGroupUrns);

    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 2);

    // Published clause should have: relatedAssets + state + draftOf IS_NULL + showInGlobalContext
    ConjunctiveCriterion publishedClause = filter.getOr().get(0);
    assertEquals(publishedClause.getAnd().size(), 4);
    assertEquals(publishedClause.getAnd().get(1).getField(), "state");
    assertEquals(publishedClause.getAnd().get(1).getValues().get(0), "PUBLISHED");
    assertEquals(publishedClause.getAnd().get(2).getField(), "draftOf");
    assertEquals(publishedClause.getAnd().get(2).getCondition(), Condition.IS_NULL);
    assertEquals(publishedClause.getAnd().get(3).getField(), "showInGlobalContext");
    assertEquals(publishedClause.getAnd().get(3).getValues().get(0), "true");

    // Unpublished clause should have: relatedAssets + state + owners + draftOf IS_NULL +
    // showInGlobalContext
    ConjunctiveCriterion unpublishedClause = filter.getOr().get(1);
    assertEquals(unpublishedClause.getAnd().size(), 5);
    assertEquals(unpublishedClause.getAnd().get(1).getField(), "state");
    assertEquals(unpublishedClause.getAnd().get(1).getValues().get(0), "UNPUBLISHED");
    assertEquals(unpublishedClause.getAnd().get(2).getField(), "owners");
    assertEquals(unpublishedClause.getAnd().get(3).getField(), "draftOf");
    assertEquals(unpublishedClause.getAnd().get(3).getCondition(), Condition.IS_NULL);
    assertEquals(unpublishedClause.getAnd().get(4).getField(), "showInGlobalContext");
    assertEquals(unpublishedClause.getAnd().get(4).getValues().get(0), "true");
  }

  @Test
  public void testBuildCombinedFilterExcludesDrafts() {
    // Verify that drafts are excluded from both published and unpublished clauses
    List<Criterion> baseCriteria = new ArrayList<>();
    List<String> userAndGroupUrns = ImmutableList.of(TEST_USER_URN);

    Filter filter = DocumentSearchFilterUtils.buildCombinedFilter(baseCriteria, userAndGroupUrns);

    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 2);

    // Both clauses should have draftOf IS_NULL criterion
    boolean foundDraftOfInPublished = false;
    boolean foundDraftOfInUnpublished = false;

    ConjunctiveCriterion publishedClause = filter.getOr().get(0);
    for (Criterion criterion : publishedClause.getAnd()) {
      if ("draftOf".equals(criterion.getField()) && criterion.getCondition() == Condition.IS_NULL) {
        foundDraftOfInPublished = true;
      }
    }

    ConjunctiveCriterion unpublishedClause = filter.getOr().get(1);
    for (Criterion criterion : unpublishedClause.getAnd()) {
      if ("draftOf".equals(criterion.getField()) && criterion.getCondition() == Condition.IS_NULL) {
        foundDraftOfInUnpublished = true;
      }
    }

    assertTrue(foundDraftOfInPublished, "Published clause should exclude drafts (draftOf IS_NULL)");
    assertTrue(
        foundDraftOfInUnpublished, "Unpublished clause should exclude drafts (draftOf IS_NULL)");
  }

  @Test
  public void testBuildCombinedFilterRequiresShowInGlobalContext() {
    // Verify that showInGlobalContext=true is required in both published and unpublished clauses
    List<Criterion> baseCriteria = new ArrayList<>();
    List<String> userAndGroupUrns = ImmutableList.of(TEST_USER_URN);

    Filter filter = DocumentSearchFilterUtils.buildCombinedFilter(baseCriteria, userAndGroupUrns);

    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 2);

    // Both clauses should have showInGlobalContext=true criterion
    boolean foundShowInGlobalContextInPublished = false;
    boolean foundShowInGlobalContextInUnpublished = false;

    ConjunctiveCriterion publishedClause = filter.getOr().get(0);
    for (Criterion criterion : publishedClause.getAnd()) {
      if ("showInGlobalContext".equals(criterion.getField())
          && criterion.getCondition() == Condition.EQUAL
          && criterion.getValues().contains("true")) {
        foundShowInGlobalContextInPublished = true;
      }
    }

    ConjunctiveCriterion unpublishedClause = filter.getOr().get(1);
    for (Criterion criterion : unpublishedClause.getAnd()) {
      if ("showInGlobalContext".equals(criterion.getField())
          && criterion.getCondition() == Condition.EQUAL
          && criterion.getValues().contains("true")) {
        foundShowInGlobalContextInUnpublished = true;
      }
    }

    assertTrue(
        foundShowInGlobalContextInPublished,
        "Published clause should require showInGlobalContext=true");
    assertTrue(
        foundShowInGlobalContextInUnpublished,
        "Unpublished clause should require showInGlobalContext=true");
  }
}
