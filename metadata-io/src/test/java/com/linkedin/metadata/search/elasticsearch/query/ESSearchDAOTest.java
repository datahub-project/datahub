package com.linkedin.metadata.search.elasticsearch.query;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.ESSampleDataFixture;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.linkedin.metadata.query.filter.Criterion;
import org.springframework.beans.factory.annotation.Qualifier;

import static org.testng.Assert.*;


@Import(ESSampleDataFixture.class)
public class ESSearchDAOTest extends AbstractTestNGSpringContextTests {
  @Autowired
  @Qualifier("sampleDataIndexConvention")
  IndexConvention indexConvention;

  @Test
  public void testTransformFilterForEntitiesNoChange() {
    Criterion c = new Criterion().setValue("urn:li:tag:abc").setValues(
        new StringArray(ImmutableList.of("urn:li:tag:abc", "urn:li:tag:def"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("tags.keyword");

    Filter f = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(c))));

    Filter transformedFilter = ESSearchDAO.transformFilterForEntities(f, indexConvention);
    assertEquals(f, transformedFilter);
  }

  @Test
  public void testTransformFilterForEntitiesNullFilter() {
    Filter transformedFilter = ESSearchDAO.transformFilterForEntities(null, indexConvention);
    assertNotNull(indexConvention);
    assertEquals(null, transformedFilter);
  }

  @Test
  public void testTransformFilterForEntitiesWithChanges() {

    Criterion c = new Criterion().setValue("dataset").setValues(
        new StringArray(ImmutableList.of("dataset"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("_entityType");

    Filter f = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    Filter originalF = null;
    try {
      originalF = f.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(f, originalF);

    Filter transformedFilter = ESSearchDAO.transformFilterForEntities(f, indexConvention);
    assertNotEquals(originalF, transformedFilter);

    Criterion expectedNewCriterion = new Criterion().setValue("smpldat_datasetindex_v2").setValues(
        new StringArray(ImmutableList.of("smpldat_datasetindex_v2"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("_index");

    Filter expectedNewFilter = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(expectedNewCriterion))));

    assertEquals(expectedNewFilter, transformedFilter);
  }
}