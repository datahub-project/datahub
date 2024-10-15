package com.linkedin.datahub.graphql.types.form;

import static org.testng.Assert.*;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import org.testng.annotations.Test;

public class FilterConverterTest {
  @Test
  void testSimpleFilter() throws Exception {
    Filter filter = new Filter();
    ConjunctiveCriterion cc = new ConjunctiveCriterion();
    Criterion criterion1 = new Criterion();
    criterion1.setField("platform");
    criterion1.setValue("urn:li:dataPlatform:bigquery");
    criterion1.setValues(
        new StringArray("urn:li:dataPlatform:bigquery", "urn:li:dataPlatform:snowflake"));
    criterion1.setCondition(Condition.EQUAL);

    Criterion criterion2 = new Criterion();
    criterion2.setField("_entityType");
    criterion2.setValue("dataset");
    criterion2.setValues(new StringArray("dataset", "container"));
    criterion2.setCondition(Condition.EQUAL);

    cc.setAnd(new CriterionArray(criterion1, criterion2));
    filter.setOr(new ConjunctiveCriterionArray(cc));

    String expected =
        "{\"operator\":\"and\",\"operands\":[{\"property\":\"platform\",\"operator\":\"equals\",\"values\":[\"urn:li:dataPlatform:bigquery\",\"urn:li:dataPlatform:snowflake\"]},{\"property\":\"_entityType\",\"operator\":\"equals\",\"values\":[\"dataset\",\"container\"]}]}";
    String result = FilterConverter.convertFilterToJsonPredicate(filter);

    assertEquals(expected, result);
  }

  @Test
  void testComplexFilter() throws Exception {
    Filter filter = new Filter();
    ConjunctiveCriterion cc1 = new ConjunctiveCriterion();
    ConjunctiveCriterion cc2 = new ConjunctiveCriterion();
    ConjunctiveCriterion cc3 = new ConjunctiveCriterion();

    Criterion criterion1 = createCriterion("_entityType", "dataset", Condition.EQUAL);
    Criterion criterion2 =
        createCriterion("platform.keyword", "urn:li:dataPlatform:snowflake", Condition.EQUAL);
    Criterion criterion3 =
        createCriterion("domains.keyword", "urn:li:domain:marketing", Condition.EQUAL);
    Criterion criterion4 =
        createCriterion(
            "container", "urn:li:container:b41c14bc5cb3ccfbb0433c8cbdef2992", Condition.EQUAL);
    Criterion criterion5 =
        createCriterion(
            "container.keyword",
            "urn:li:container:701919de0ec93cb338fe9bac0b35403c",
            Condition.EQUAL);
    Criterion criterion6 =
        createCriterion(
            "container.keyword",
            "urn:li:container:0e859a5f696621d574d8ade738aacd4f",
            Condition.EQUAL);

    cc1.setAnd(new CriterionArray(criterion1, criterion2, criterion3, criterion4));
    cc2.setAnd(new CriterionArray(criterion1, criterion2, criterion3, criterion5));
    cc3.setAnd(new CriterionArray(criterion1, criterion2, criterion3, criterion6));

    filter.setOr(new ConjunctiveCriterionArray(cc1, cc2, cc3));

    String expected =
        "{\"operator\":\"or\",\"operands\":[{\"operator\":\"and\",\"operands\":[{\"property\":\"_entityType\",\"operator\":\"equals\",\"values\":[\"dataset\"]},{\"property\":\"platform\",\"operator\":\"equals\",\"values\":[\"urn:li:dataPlatform:snowflake\"]},{\"property\":\"domains\",\"operator\":\"equals\",\"values\":[\"urn:li:domain:marketing\"]},{\"property\":\"container\",\"operator\":\"equals\",\"values\":[\"urn:li:container:b41c14bc5cb3ccfbb0433c8cbdef2992\"]}]},{\"operator\":\"and\",\"operands\":[{\"property\":\"_entityType\",\"operator\":\"equals\",\"values\":[\"dataset\"]},{\"property\":\"platform\",\"operator\":\"equals\",\"values\":[\"urn:li:dataPlatform:snowflake\"]},{\"property\":\"domains\",\"operator\":\"equals\",\"values\":[\"urn:li:domain:marketing\"]},{\"property\":\"container\",\"operator\":\"equals\",\"values\":[\"urn:li:container:701919de0ec93cb338fe9bac0b35403c\"]}]},{\"operator\":\"and\",\"operands\":[{\"property\":\"_entityType\",\"operator\":\"equals\",\"values\":[\"dataset\"]},{\"property\":\"platform\",\"operator\":\"equals\",\"values\":[\"urn:li:dataPlatform:snowflake\"]},{\"property\":\"domains\",\"operator\":\"equals\",\"values\":[\"urn:li:domain:marketing\"]},{\"property\":\"container\",\"operator\":\"equals\",\"values\":[\"urn:li:container:0e859a5f696621d574d8ade738aacd4f\"]}]}]}";
    String result = FilterConverter.convertFilterToJsonPredicate(filter);

    assertEquals(expected, result);
  }

  @Test
  void testNegatedCondition() throws Exception {
    Filter filter = new Filter();
    ConjunctiveCriterion cc = new ConjunctiveCriterion();
    Criterion criterion = new Criterion();
    criterion.setField("status");
    criterion.setValue("active");
    criterion.setValues(new StringArray("active"));
    criterion.setCondition(Condition.EQUAL);
    criterion.setNegated(true);

    cc.setAnd(new CriterionArray(criterion));
    filter.setOr(new ConjunctiveCriterionArray(cc));

    String expected =
        "{\"operator\":\"and\",\"operands\":[{\"property\":\"status\",\"operator\":\"not_equals\",\"values\":[\"active\"]}]}";
    String result = FilterConverter.convertFilterToJsonPredicate(filter);

    assertEquals(expected, result);
  }

  @Test
  void testEmptyFilter() throws Exception {
    Filter filter = new Filter();

    String expected = "{}";
    String result = FilterConverter.convertFilterToJsonPredicate(filter);

    assertEquals(expected, result);
  }

  private Criterion createCriterion(String field, String value, Condition condition) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setValue(value);
    criterion.setValues(new StringArray(value));
    criterion.setCondition(condition);
    return criterion;
  }
}
