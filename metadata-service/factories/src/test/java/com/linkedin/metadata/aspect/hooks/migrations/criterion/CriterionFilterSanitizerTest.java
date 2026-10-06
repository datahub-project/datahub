package com.linkedin.metadata.aspect.hooks.migrations.criterion;

import static com.linkedin.metadata.Constants.DATAHUB_VIEW_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.view.DataHubViewInfo;
import org.testng.annotations.Test;

public class CriterionFilterSanitizerTest {

  @Test
  public void sanitizeFilterDataMap_nullFilter_noOp() {
    assertFalse(CriterionFilterMutatorBase.sanitizeFilterDataMap(null));
  }

  @Test
  public void sanitizeFilterDataMap_criteriaRemovesValueAndCoalescesToValues() {
    DataMap criterion = new DataMap();
    criterion.put("field", "foo");
    criterion.put("value", "bar");

    DataList criteria = new DataList();
    criteria.add(criterion);

    DataMap filter = new DataMap();
    filter.put("criteria", criteria);

    assertTrue(CriterionFilterMutatorBase.sanitizeFilterDataMap(filter));
    assertFalse(criterion.containsKey("value"));
    DataList values = criterion.getDataList("values");
    assertEquals(values.size(), 1);
    assertEquals(values.get(0), "bar");
  }

  @Test
  public void sanitizeFilterDataMap_criteriaAppendsValueWhenNotAlreadyInValues() {
    DataMap criterion = new DataMap();
    criterion.put("field", "foo");
    criterion.put("value", "ignored");
    DataList existing = new DataList();
    existing.add("x");
    criterion.put("values", existing);

    DataList criteria = new DataList();
    criteria.add(criterion);

    DataMap filter = new DataMap();
    filter.put("criteria", criteria);

    assertTrue(CriterionFilterMutatorBase.sanitizeFilterDataMap(filter));
    assertFalse(criterion.containsKey("value"));
    assertEquals(criterion.getDataList("values").size(), 2);
    assertEquals(criterion.getDataList("values").get(0), "x");
    assertEquals(criterion.getDataList("values").get(1), "ignored");
  }

  @Test
  public void sanitizeFilterDataMap_orAndCriteria() {
    DataMap inner = new DataMap();
    inner.put("field", "k");
    inner.put("value", "v");

    DataList and = new DataList();
    and.add(inner);

    DataMap conjunct = new DataMap();
    conjunct.put("and", and);

    DataList or = new DataList();
    or.add(conjunct);

    DataMap filter = new DataMap();
    filter.put("or", or);

    assertTrue(CriterionFilterMutatorBase.sanitizeFilterDataMap(filter));
    assertFalse(inner.containsKey("value"));
    assertEquals(inner.getDataList("values").get(0), "v");
  }

  @Test
  public void migrateLegacyCriterionValue_valueAlreadyPresentInValues_dropsValueOnly() {
    DataMap criterion = new DataMap();
    criterion.put("field", "foo");
    criterion.put("value", "bar");
    DataList values = new DataList();
    values.add("bar");
    criterion.put("values", values);

    assertTrue(CriterionFilterMutatorBase.migrateLegacyCriterionValue(criterion));
    assertFalse(criterion.containsKey("value"));
    assertEquals(values.size(), 1);
    assertEquals(values.get(0), "bar");
  }

  @Test
  public void migrateLegacyCriterionValue_appendsWhenValuesMissingSameString() {
    DataMap criterion = new DataMap();
    criterion.put("field", "foo");
    criterion.put("value", "bar");

    assertTrue(CriterionFilterMutatorBase.migrateLegacyCriterionValue(criterion));
    assertFalse(criterion.containsKey("value"));
    DataList values = criterion.getDataList("values");
    assertEquals(values.size(), 1);
    assertEquals(values.get(0), "bar");
  }

  @Test
  public void migrateLegacyCriterionValue_noValue_returnsFalse() {
    DataMap criterion = new DataMap();
    criterion.put("field", "f");
    assertFalse(CriterionFilterMutatorBase.migrateLegacyCriterionValue(criterion));
  }

  @Test
  public void migrateLegacyCriterionValue_nonStringValue_stillDropsKey() {
    DataMap criterion = new DataMap();
    criterion.put("field", "f");
    criterion.put("value", 123);
    assertTrue(CriterionFilterMutatorBase.migrateLegacyCriterionValue(criterion));
    assertFalse(criterion.containsKey("value"));
    assertTrue(criterion.getDataList("values").isEmpty());
  }

  @Test
  public void migrateLegacyCriterionValue_emptyStringValue_addsExplicitEmptyValues() {
    DataMap criterion = new DataMap();
    criterion.put("field", "f");
    criterion.put("value", "");
    assertTrue(CriterionFilterMutatorBase.migrateLegacyCriterionValue(criterion));
    assertFalse(criterion.containsKey("value"));
    assertTrue(criterion.containsKey("values"));
    assertTrue(criterion.getDataList("values").isEmpty());
  }

  @Test
  public void isSupportedAspect_returnsFalseForUnknownAspects() {
    // Both supported aspects must short-circuit to true; everything else must be false.
    assertTrue(CriterionFilterMutatorBase.isSupportedAspect(DATAHUB_VIEW_INFO_ASPECT_NAME));
    assertTrue(CriterionFilterMutatorBase.isSupportedAspect(DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME));
    assertFalse(CriterionFilterMutatorBase.isSupportedAspect("ownership"));
  }

  @Test
  public void sanitizeInPlaceOnCopy_unsupportedAspect_returnsFalse() {
    DataHubViewInfo view = new DataHubViewInfo();
    assertFalse(CriterionFilterMutatorBase.sanitizeInPlaceOnCopy("ownership", view));
  }

  @Test
  public void sanitizeInPlaceOnCopy_viewInfoMissingDefinition_returnsFalse() {
    // dataHubViewInfo with no `definition` field — defensive branch in the static migration helper.
    DataHubViewInfo view = new DataHubViewInfo();
    assertFalse(
        CriterionFilterMutatorBase.sanitizeInPlaceOnCopy(DATAHUB_VIEW_INFO_ASPECT_NAME, view));
  }

  @Test
  public void sanitizeFilterDataMap_orListWithNonDataMapEntry_isIgnored() {
    DataMap inner = new DataMap();
    inner.put("field", "k");
    inner.put("value", "v");

    DataList and = new DataList();
    and.add(inner);

    DataMap conjunct = new DataMap();
    conjunct.put("and", and);

    DataList or = new DataList();
    or.add("not-a-map"); // non-DataMap entry must be skipped without aborting iteration
    or.add(conjunct);

    DataMap filter = new DataMap();
    filter.put("or", or);

    assertTrue(CriterionFilterMutatorBase.sanitizeFilterDataMap(filter));
    assertFalse(inner.containsKey("value"));
  }

  @Test
  public void sanitizeFilterDataMap_criteriaListWithNonDataMapEntry_isIgnored() {
    DataMap real = new DataMap();
    real.put("field", "k");
    real.put("value", "v");

    DataList criteria = new DataList();
    criteria.add("not-a-map"); // non-DataMap entry must be skipped
    criteria.add(real);

    DataMap filter = new DataMap();
    filter.put("criteria", criteria);

    assertTrue(CriterionFilterMutatorBase.sanitizeFilterDataMap(filter));
    assertFalse(real.containsKey("value"));
  }

  @Test
  public void sanitizeFilterDataMap_conjunctWithoutAndList_isIgnored() {
    // Conjunct exists in `or` but has no `and` list → outer call must return false (no work).
    DataMap conjunct = new DataMap();

    DataList or = new DataList();
    or.add(conjunct);

    DataMap filter = new DataMap();
    filter.put("or", or);

    assertFalse(CriterionFilterMutatorBase.sanitizeFilterDataMap(filter));
  }
}
