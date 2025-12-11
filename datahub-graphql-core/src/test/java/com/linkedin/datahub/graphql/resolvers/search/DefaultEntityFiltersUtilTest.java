package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import java.util.List;
import org.testng.annotations.Test;

public class DefaultEntityFiltersUtilTest {

  @Test
  public void testAddDefaultEntityFilters_NoDocument() {
    // When DOCUMENT is not in the entity types, no filters should be added
    List<String> entityTypes = ImmutableList.of(DATASET_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true);

    assertNull(result);
  }

  @Test
  public void testAddDefaultEntityFilters_NoDocumentWithBaseFilter() {
    // When DOCUMENT is not in entity types, base filter should be returned unchanged
    List<String> entityTypes = ImmutableList.of(DATASET_ENTITY_NAME);
    Filter baseFilter = new Filter();

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(baseFilter, entityTypes, true);

    assertSame(result, baseFilter);
  }

  @Test
  public void testAddDefaultEntityFilters_WithDocumentAndShowInGlobalContext() {
    // When DOCUMENT is in entity types with showInGlobalContext=true,
    // should add document filters using negated EQUAL for cross-entity compatibility
    List<String> entityTypes = ImmutableList.of(DATASET_ENTITY_NAME, DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true);

    assertNotNull(result);
    assertNotNull(result.getOr());
    assertEquals(result.getOr().size(), 1); // Single AND clause with negated conditions

    ConjunctiveCriterion clause = result.getOr().get(0);
    assertEquals(clause.getAnd().size(), 3); // state, showInGlobalContext, draftOf

    // Verify negated EQUAL conditions (these pass for non-documents since field doesn't exist)
    assertTrue(hasNegatedFieldWithCondition(clause, "state", Condition.EQUAL));
    assertTrue(hasNegatedFieldWithCondition(clause, "showInGlobalContext", Condition.EQUAL));
    assertTrue(hasFieldWithCondition(clause, "draftOf", Condition.IS_NULL));
  }

  @Test
  public void testAddDefaultEntityFilters_WithDocumentWithoutShowInGlobalContext() {
    // When DOCUMENT is in entity types with showInGlobalContext=false,
    // should add document filters without showInGlobalContext criterion
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, false);

    assertNotNull(result);
    assertNotNull(result.getOr());
    assertEquals(result.getOr().size(), 1); // Single AND clause

    ConjunctiveCriterion clause = result.getOr().get(0);
    assertEquals(clause.getAnd().size(), 2); // state, draftOf (no showInGlobalContext)

    assertTrue(hasNegatedFieldWithCondition(clause, "state", Condition.EQUAL));
    assertFalse(hasField(clause, "showInGlobalContext"));
    assertTrue(hasFieldWithCondition(clause, "draftOf", Condition.IS_NULL));
  }

  @Test
  public void testAddDefaultEntityFilters_DocumentOnlyEntity() {
    // When only DOCUMENT is being searched
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true);

    assertNotNull(result);
    assertEquals(result.getOr().size(), 1); // Single AND clause
  }

  @Test
  public void testAddDefaultEntityFilters_VerifyFilterValues() {
    // Verify the actual filter values are correct
    List<String> entityTypes = ImmutableList.of(DOCUMENT_ENTITY_NAME);

    Filter result = DefaultEntityFiltersUtil.addDefaultEntityFilters(null, entityTypes, true);

    ConjunctiveCriterion clause = result.getOr().get(0);

    // Verify state != UNPUBLISHED (negated EQUAL)
    Criterion stateCriterion = findCriterion(clause, "state");
    assertNotNull(stateCriterion);
    assertEquals(stateCriterion.getCondition(), Condition.EQUAL);
    assertTrue(stateCriterion.isNegated());
    assertTrue(stateCriterion.getValues().contains("UNPUBLISHED"));

    // Verify showInGlobalContext != false (negated EQUAL)
    Criterion showInGlobalContextCriterion = findCriterion(clause, "showInGlobalContext");
    assertNotNull(showInGlobalContextCriterion);
    assertEquals(showInGlobalContextCriterion.getCondition(), Condition.EQUAL);
    assertTrue(showInGlobalContextCriterion.isNegated());
    assertTrue(showInGlobalContextCriterion.getValues().contains("false"));

    // Verify draftOf IS_NULL
    Criterion draftOfCriterion = findCriterion(clause, "draftOf");
    assertNotNull(draftOfCriterion);
    assertEquals(draftOfCriterion.getCondition(), Condition.IS_NULL);
    assertFalse(draftOfCriterion.isNegated());
  }

  private static boolean hasField(ConjunctiveCriterion clause, String field) {
    return findCriterion(clause, field) != null;
  }

  private static boolean hasFieldWithCondition(
      ConjunctiveCriterion clause, String field, Condition condition) {
    Criterion criterion = findCriterion(clause, field);
    return criterion != null
        && condition.equals(criterion.getCondition())
        && !criterion.isNegated();
  }

  private static boolean hasNegatedFieldWithCondition(
      ConjunctiveCriterion clause, String field, Condition condition) {
    Criterion criterion = findCriterion(clause, field);
    return criterion != null && condition.equals(criterion.getCondition()) && criterion.isNegated();
  }

  private static Criterion findCriterion(ConjunctiveCriterion clause, String field) {
    for (Criterion criterion : clause.getAnd()) {
      if (field.equals(criterion.getField())) {
        return criterion;
      }
    }
    return null;
  }
}
