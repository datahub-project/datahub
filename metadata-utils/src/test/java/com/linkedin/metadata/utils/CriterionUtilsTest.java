package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import org.testng.annotations.Test;

public class CriterionUtilsTest {
  @Test
  public void testNullFilter() {
    Filter result = CriterionUtils.validateAndConvert(null);
    assertNull(result);
  }

  @Test
  public void testEmptyFilter() {
    Filter input = new Filter();
    Filter result = CriterionUtils.validateAndConvert(input);
    assertNotNull(result);
    assertFalse(result.hasCriteria());
    assertFalse(result.hasOr());
  }

  @Test
  public void testSimpleCriterionConversion() {
    Filter input = new Filter();
    Criterion criterion = new Criterion();
    criterion.setValue("testValue");
    input.setCriteria(new CriterionArray(criterion));

    Filter result = CriterionUtils.validateAndConvert(input);

    Criterion convertedCriterion = result.getCriteria().get(0);
    assertEquals(convertedCriterion.getValue(), "");
    assertTrue(convertedCriterion.hasValues());
    assertEquals("testValue", convertedCriterion.getValues().get(0));
  }

  @Test
  public void testOrClauseCriterionConversion() {
    Filter input = new Filter();

    // Create OR clause with AND criteria
    Criterion criterion = new Criterion();
    criterion.setValue("orValue");

    ConjunctiveCriterion conjunctive = new ConjunctiveCriterion();
    conjunctive.setAnd(new CriterionArray(criterion));

    input.setOr(new ConjunctiveCriterionArray(conjunctive));

    Filter result = CriterionUtils.validateAndConvert(input);

    Criterion convertedCriterion = result.getOr().get(0).getAnd().get(0);
    assertEquals(convertedCriterion.getValue(), "");
    assertTrue(convertedCriterion.hasValues());
    assertEquals("orValue", convertedCriterion.getValues().get(0));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCommaInValueThrowsException() {
    Filter input = new Filter();
    Criterion criterion = new Criterion();
    criterion.setValue("value1,value2");
    input.setCriteria(new CriterionArray(criterion));

    CriterionUtils.validateAndConvert(input);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConflictingValuesThrowsException() {
    Filter input = new Filter();
    Criterion criterion = new Criterion();
    criterion.setValue("value1");
    criterion.setValues(new StringArray("differentValue"));
    input.setCriteria(new CriterionArray(criterion));

    CriterionUtils.validateAndConvert(input);
  }

  @Test
  public void testExistingValuesNotModified() {
    Filter input = new Filter();
    Criterion criterion = new Criterion();
    criterion.setValue("value1");
    criterion.setValues(new StringArray("value1")); // Same value, should not throw exception
    input.setCriteria(new CriterionArray(criterion));

    Filter result = CriterionUtils.validateAndConvert(input);

    Criterion convertedCriterion = result.getCriteria().get(0);
    assertEquals(convertedCriterion.getValue(), "");
    assertTrue(convertedCriterion.hasValues());
    assertEquals("value1", convertedCriterion.getValues().get(0));
  }

  @Test
  public void testMultipleCriteriaConversion() {
    Filter input = new Filter();

    Criterion criterion1 = new Criterion();
    criterion1.setValue("value1");

    Criterion criterion2 = new Criterion();
    criterion2.setValue("value2");

    input.setCriteria(new CriterionArray(criterion1, criterion2));

    Filter result = CriterionUtils.validateAndConvert(input);

    assertEquals(2, result.getCriteria().size());

    for (Criterion c : result.getCriteria()) {
      assertEquals(c.getValue(), "");
      assertTrue(c.hasValues());
      assertTrue(c.getValues().get(0).equals("value1") || c.getValues().get(0).equals("value2"));
    }
  }

  @Test
  public void testMixedCriteriaAndOrClause() {
    Filter input = new Filter();

    // Add direct criteria
    Criterion criterion1 = new Criterion();
    criterion1.setValue("directValue");
    input.setCriteria(new CriterionArray(criterion1));

    // Add OR clause with AND criteria
    Criterion criterion2 = new Criterion();
    criterion2.setValue("orValue");
    ConjunctiveCriterion conjunctive = new ConjunctiveCriterion();
    conjunctive.setAnd(new CriterionArray(criterion2));
    input.setOr(new ConjunctiveCriterionArray(conjunctive));

    Filter result = CriterionUtils.validateAndConvert(input);

    // Check direct criterion
    Criterion convertedDirect = result.getCriteria().get(0);
    assertEquals(convertedDirect.getValue(), "");
    assertTrue(convertedDirect.hasValues());
    assertEquals("directValue", convertedDirect.getValues().get(0));

    // Check OR clause criterion
    Criterion convertedOr = result.getOr().get(0).getAnd().get(0);
    assertEquals(convertedOr.getValue(), "");
    assertTrue(convertedOr.hasValues());
    assertEquals("orValue", convertedOr.getValues().get(0));
  }

  @Test
  public void testEmptyStringValueNotConverted() {
    Filter input = new Filter();
    Criterion criterion = new Criterion();
    criterion.setValue(""); // Empty string value
    input.setCriteria(new CriterionArray(criterion));

    Filter result = CriterionUtils.validateAndConvert(input);

    Criterion convertedCriterion = result.getCriteria().get(0);
    assertEquals(convertedCriterion.getValue(), "");
    assertFalse(convertedCriterion.hasValues()); // Should not be converted since value was empty
  }

  @Test
  public void testMixedEmptyAndNonEmptyValues() {
    Filter input = new Filter();

    Criterion emptyCriterion = new Criterion();
    emptyCriterion.setValue("");

    Criterion nonEmptyCriterion = new Criterion();
    nonEmptyCriterion.setValue("value1");

    input.setCriteria(new CriterionArray(emptyCriterion, nonEmptyCriterion));

    Filter result = CriterionUtils.validateAndConvert(input);

    assertEquals(2, result.getCriteria().size());

    // Check empty criterion
    Criterion convertedEmpty = result.getCriteria().get(0);
    assertEquals(convertedEmpty.getValue(), "");
    assertFalse(convertedEmpty.hasValues());

    // Check non-empty criterion
    Criterion convertedNonEmpty = result.getCriteria().get(1);
    assertEquals(convertedNonEmpty.getValue(), "");
    assertTrue(convertedNonEmpty.hasValues());
    assertEquals(convertedNonEmpty.getValues().get(0), "value1");
  }

  @Test
  public void testOrClauseWithEmptyValues() {
    Filter input = new Filter();

    // Create OR clause with mixed empty and non-empty criteria
    Criterion emptyCriterion = new Criterion();
    emptyCriterion.setValue("");

    Criterion nonEmptyCriterion = new Criterion();
    nonEmptyCriterion.setValue("orValue");

    ConjunctiveCriterion conjunctive = new ConjunctiveCriterion();
    conjunctive.setAnd(new CriterionArray(emptyCriterion, nonEmptyCriterion));

    input.setOr(new ConjunctiveCriterionArray(conjunctive));

    Filter result = CriterionUtils.validateAndConvert(input);

    // Check empty criterion
    Criterion convertedEmpty = result.getOr().get(0).getAnd().get(0);
    assertEquals(convertedEmpty.getValue(), "");
    assertFalse(convertedEmpty.hasValues());

    // Check non-empty criterion
    Criterion convertedNonEmpty = result.getOr().get(0).getAnd().get(1);
    assertEquals(convertedNonEmpty.getValue(), "");
    assertTrue(convertedNonEmpty.hasValues());
    assertEquals(convertedNonEmpty.getValues().get(0), "orValue");
  }

  @Test
  public void testCriterionWithOnlyValues() {
    Filter input = new Filter();
    Criterion criterion = new Criterion();
    criterion.setValues(new StringArray("value1")); // Only has values, no value field set
    input.setCriteria(new CriterionArray(criterion));

    Filter result = CriterionUtils.validateAndConvert(input);

    Criterion convertedCriterion = result.getCriteria().get(0);
    assertEquals(convertedCriterion.getValue(), "");
    assertTrue(convertedCriterion.hasValues());
    assertEquals(convertedCriterion.getValues().get(0), "value1");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMultiUrnThrowsException() {
    Filter input = new Filter();
    Criterion criterion = new Criterion();
    criterion.setValue(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,foo,PROD),urn:li:dataset:(urn:li:dataPlatform:postgres,foo,PROD)");
    input.setCriteria(new CriterionArray(criterion));

    CriterionUtils.validateAndConvert(input);
  }

  @Test
  public void testUrnConversion() {
    Filter input = new Filter();
    Criterion criterion = new Criterion();
    criterion.setValue("urn:li:dataset:(urn:li:dataPlatform:postgres,foo,PROD)");
    input.setCriteria(new CriterionArray(criterion));

    Filter result = CriterionUtils.validateAndConvert(input);

    Criterion convertedCriterion = result.getCriteria().get(0);
    assertEquals(convertedCriterion.getValue(), "");
    assertTrue(convertedCriterion.hasValues());
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,foo,PROD)",
        convertedCriterion.getValues().get(0));
  }
}
