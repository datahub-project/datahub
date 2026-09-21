package com.linkedin.datahub.graphql.types.view;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LogicalOperator;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import org.testng.annotations.Test;

public class DataHubViewMapperTest {

  private static final String TEST_VIEW_URN = "urn:li:dataHubView:test-view";
  private static final String TEST_VIEW_NAME = "Test View";
  private static final String TEST_VIEW_DESCRIPTION = "Test view description";
  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)";

  // Basic Mapping Tests

  @Test
  public void testMapViewUrn() throws Exception {
    // Test that URN is correctly mapped
    EntityResponse response = buildViewEntityResponse(null, buildSimpleFilter());
    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_VIEW_URN);
  }

  @Test
  public void testMapViewInfo() throws Exception {
    // Test that view name, description, and type are mapped
    EntityResponse response = buildViewEntityResponse(null, buildSimpleFilter());
    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result);
    assertEquals(result.getType(), EntityType.DATAHUB_VIEW);
    assertEquals(result.getName(), TEST_VIEW_NAME);
    assertEquals(result.getDescription(), TEST_VIEW_DESCRIPTION);
  }

  // Filter Structure Tests

  @Test
  public void testMapViewDetectsAndOperator() throws Exception {
    // Test that AND operator is detected when filter has single OR with multiple ANDs
    Filter andFilter = new Filter();
    Criterion criterion1 = new Criterion();
    criterion1.setField("field1");
    criterion1.setCondition(Condition.EQUAL);
    criterion1.setValues(new com.linkedin.data.template.StringArray("value1"));

    Criterion criterion2 = new Criterion();
    criterion2.setField("field2");
    criterion2.setCondition(Condition.EQUAL);
    criterion2.setValues(new com.linkedin.data.template.StringArray("value2"));

    ConjunctiveCriterion and = new ConjunctiveCriterion();
    and.setAnd(new CriterionArray(ImmutableList.of(criterion1, criterion2)));
    andFilter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and)));

    EntityResponse response = buildViewEntityResponse(null, andFilter);
    DataHubView result = DataHubViewMapper.map(null, response);

    assertEquals(result.getDefinition().getFilter().getOperator(), LogicalOperator.AND);
  }

  @Test
  public void testMapViewDetectsOrOperator() throws Exception {
    // Test that OR operator is detected when filter has multiple ORs
    Filter orFilter = new Filter();
    Criterion criterion1 = new Criterion();
    criterion1.setField("field1");
    criterion1.setCondition(Condition.EQUAL);
    criterion1.setValues(new com.linkedin.data.template.StringArray("value1"));

    Criterion criterion2 = new Criterion();
    criterion2.setField("field2");
    criterion2.setCondition(Condition.EQUAL);
    criterion2.setValues(new com.linkedin.data.template.StringArray("value2"));

    ConjunctiveCriterion and1 = new ConjunctiveCriterion();
    and1.setAnd(new CriterionArray(ImmutableList.of(criterion1)));

    ConjunctiveCriterion and2 = new ConjunctiveCriterion();
    and2.setAnd(new CriterionArray(ImmutableList.of(criterion2)));

    orFilter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and1, and2)));

    EntityResponse response = buildViewEntityResponse(null, orFilter);
    DataHubView result = DataHubViewMapper.map(null, response);

    assertEquals(result.getDefinition().getFilter().getOperator(), LogicalOperator.OR);
  }

  // Criterion Mapping Tests

  @Test
  public void testMapCriterionWithNegation() throws Exception {
    // Test that criterion negation flag is preserved
    Filter filter = new Filter();
    Criterion criterion = new Criterion();
    criterion.setField("testField");
    criterion.setCondition(Condition.EQUAL);
    criterion.setValues(new com.linkedin.data.template.StringArray("testValue"));
    criterion.setNegated(true);

    ConjunctiveCriterion and = new ConjunctiveCriterion();
    and.setAnd(new CriterionArray(ImmutableList.of(criterion)));
    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and)));

    EntityResponse response = buildViewEntityResponse(null, filter);
    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result.getDefinition().getFilter().getFilters());
    assertTrue(result.getDefinition().getFilter().getFilters().size() > 0);
    assertTrue(result.getDefinition().getFilter().getFilters().get(0).getNegated());
  }

  @Test
  public void testStripKeywordSuffix() throws Exception {
    // Test that .keyword suffix is stripped from field names
    Filter filter = new Filter();
    Criterion criterion = new Criterion();
    criterion.setField("entityType.keyword");
    criterion.setCondition(Condition.EQUAL);
    criterion.setValues(new com.linkedin.data.template.StringArray("DATASET"));

    ConjunctiveCriterion and = new ConjunctiveCriterion();
    and.setAnd(new CriterionArray(ImmutableList.of(criterion)));
    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and)));

    EntityResponse response = buildViewEntityResponse(null, filter);
    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result.getDefinition().getFilter().getFilters());
    assertEquals(result.getDefinition().getFilter().getFilters().get(0).getField(), "entityType");
  }

  // JSON Handling Tests

  @Test
  public void testMapViewWithStoredJson() throws Exception {
    // Test that stored JSON is preserved when mapping a view
    String storedJson = "{\"type\":\"logical\",\"operator\":\"AND\",\"operands\":[]}";
    EntityResponse response = buildViewEntityResponse(storedJson, buildSimpleFilter());

    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result);
    assertEquals(result.getName(), TEST_VIEW_NAME);
    assertNotNull(result.getDefinition());
    assertNotNull(result.getDefinition().getFilter());
    assertEquals(result.getDefinition().getFilter().getJson(), storedJson);
  }

  @Test
  public void testMapViewGeneratesJsonFromFilter() throws Exception {
    // Test that JSON is generated from Filter when not stored
    EntityResponse response = buildViewEntityResponse(null, buildSimpleFilter());

    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result);
    assertNotNull(result.getDefinition());
    assertNotNull(result.getDefinition().getFilter());
    // JSON should be auto-generated from Filter via FilterConverter
    assertNotNull(result.getDefinition().getFilter().getJson());
    // Should contain valid JSON structure (starts with { and ends with })
    String json = result.getDefinition().getFilter().getJson();
    assertTrue(json.startsWith("{") && json.endsWith("}"), "JSON should be valid");
  }

  @Test
  public void testMapViewPreservesOperatorAndFilters() throws Exception {
    // Test backward compatibility: old format fields are still returned
    EntityResponse response = buildViewEntityResponse(null, buildSimpleFilter());

    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result);
    assertNotNull(result.getDefinition().getFilter());
    assertEquals(
        result.getDefinition().getFilter().getOperator(),
        com.linkedin.datahub.graphql.generated.LogicalOperator.AND);
    assertNotNull(result.getDefinition().getFilter().getFilters());
    assertTrue(result.getDefinition().getFilter().getFilters().size() > 0);
  }

  @Test
  public void testMapViewWithComplexFilter() throws Exception {
    // Test with more complex filter structure
    Filter complexFilter = new Filter();
    Criterion criterion1 = new Criterion();
    criterion1.setField("field1");
    criterion1.setCondition(Condition.EQUAL);
    criterion1.setValues(new com.linkedin.data.template.StringArray("value1"));

    Criterion criterion2 = new Criterion();
    criterion2.setField("field2");
    criterion2.setCondition(Condition.CONTAIN);
    criterion2.setValues(new com.linkedin.data.template.StringArray("value2"));
    criterion2.setNegated(true);

    ConjunctiveCriterion and = new ConjunctiveCriterion();
    and.setAnd(new CriterionArray(ImmutableList.of(criterion1, criterion2)));

    complexFilter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and)));

    EntityResponse response = buildViewEntityResponse(null, complexFilter);

    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result);
    assertNotNull(result.getDefinition().getFilter());
    // Should handle complex filters without errors
    assertNotNull(result.getDefinition().getFilter().getFilters());
    assertNotNull(result.getDefinition().getFilter().getJson());
  }

  @Test
  public void testMapViewJsonIsStringified() throws Exception {
    // Test that JSON field contains properly stringified JSON
    String storedJson = "{\"type\":\"logical\",\"operator\":\"AND\",\"operands\":[]}";
    EntityResponse response = buildViewEntityResponse(storedJson, buildSimpleFilter());

    DataHubView result = DataHubViewMapper.map(null, response);

    String json = result.getDefinition().getFilter().getJson();
    assertNotNull(json);
    // Should be a valid JSON string (can be parsed)
    assertTrue(json.startsWith("{") && json.endsWith("}"));
  }

  @Test
  public void testMapViewMultipleFilters() throws Exception {
    // Test view with multiple filters in different branches
    Filter orFilter = new Filter();

    // First AND branch
    Criterion criterion1 = new Criterion();
    criterion1.setField("field1");
    criterion1.setCondition(Condition.EQUAL);
    criterion1.setValues(new com.linkedin.data.template.StringArray("value1"));

    ConjunctiveCriterion and1 = new ConjunctiveCriterion();
    and1.setAnd(new CriterionArray(ImmutableList.of(criterion1)));

    // Second AND branch
    Criterion criterion2 = new Criterion();
    criterion2.setField("field2");
    criterion2.setCondition(Condition.EQUAL);
    criterion2.setValues(new com.linkedin.data.template.StringArray("value2"));

    ConjunctiveCriterion and2 = new ConjunctiveCriterion();
    and2.setAnd(new CriterionArray(ImmutableList.of(criterion2)));

    orFilter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and1, and2)));

    EntityResponse response = buildViewEntityResponse(null, orFilter);

    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result);
    assertNotNull(result.getDefinition().getFilter());
    // Should have converted both filter branches
    assertTrue(result.getDefinition().getFilter().getFilters().size() > 0);
  }

  // Backward Compatibility Tests

  @Test
  public void testMapViewPreservesOldFormatFields() throws Exception {
    // Test that old format fields (operator, filters) are returned alongside json
    EntityResponse response = buildViewEntityResponse(null, buildSimpleFilter());
    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result.getDefinition().getFilter());
    assertNotNull(result.getDefinition().getFilter().getOperator());
    assertNotNull(result.getDefinition().getFilter().getFilters());
    assertTrue(result.getDefinition().getFilter().getFilters().size() > 0);
  }

  // Filter Condition Tests

  @Test
  public void testMapViewCriterionWithContainCondition() throws Exception {
    // Test mapping criteria with CONTAIN condition
    Filter filter = new Filter();
    Criterion criterion = new Criterion();
    criterion.setField("description");
    criterion.setCondition(Condition.CONTAIN);
    criterion.setValues(new com.linkedin.data.template.StringArray("test"));

    ConjunctiveCriterion and = new ConjunctiveCriterion();
    and.setAnd(new CriterionArray(ImmutableList.of(criterion)));
    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and)));

    EntityResponse response = buildViewEntityResponse(null, filter);
    DataHubView result = DataHubViewMapper.map(null, response);

    assertNotNull(result.getDefinition().getFilter().getFilters());
    assertEquals(result.getDefinition().getFilter().getFilters().get(0).getField(), "description");
    assertEquals(
        result.getDefinition().getFilter().getFilters().get(0).getCondition().toString(),
        "CONTAIN");
  }

  @Test
  public void testMapViewMultipleCriteria() throws Exception {
    // Test mapping multiple criteria in AND operation
    Filter filter = new Filter();
    Criterion criterion1 = new Criterion();
    criterion1.setField("field1");
    criterion1.setCondition(Condition.EQUAL);
    criterion1.setValues(new com.linkedin.data.template.StringArray("value1"));

    Criterion criterion2 = new Criterion();
    criterion2.setField("field2");
    criterion2.setCondition(Condition.CONTAIN);
    criterion2.setValues(new com.linkedin.data.template.StringArray("value2"));

    ConjunctiveCriterion and = new ConjunctiveCriterion();
    and.setAnd(new CriterionArray(ImmutableList.of(criterion1, criterion2)));
    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and)));

    EntityResponse response = buildViewEntityResponse(null, filter);
    DataHubView result = DataHubViewMapper.map(null, response);

    assertEquals(result.getDefinition().getFilter().getOperator(), LogicalOperator.AND);
    assertEquals(result.getDefinition().getFilter().getFilters().size(), 2);
  }

  @Test
  public void testMapViewOrClausesMultipleBranches() throws Exception {
    // Test OR clauses with multiple separate branches
    Filter orFilter = new Filter();

    Criterion criterion1 = new Criterion();
    criterion1.setField("status");
    criterion1.setCondition(Condition.EQUAL);
    criterion1.setValues(new com.linkedin.data.template.StringArray("ACTIVE"));

    Criterion criterion2 = new Criterion();
    criterion2.setField("status");
    criterion2.setCondition(Condition.EQUAL);
    criterion2.setValues(new com.linkedin.data.template.StringArray("PENDING"));

    ConjunctiveCriterion and1 = new ConjunctiveCriterion();
    and1.setAnd(new CriterionArray(ImmutableList.of(criterion1)));

    ConjunctiveCriterion and2 = new ConjunctiveCriterion();
    and2.setAnd(new CriterionArray(ImmutableList.of(criterion2)));

    orFilter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and1, and2)));

    EntityResponse response = buildViewEntityResponse(null, orFilter);
    DataHubView result = DataHubViewMapper.map(null, response);

    assertEquals(result.getDefinition().getFilter().getOperator(), LogicalOperator.OR);
    assertEquals(result.getDefinition().getFilter().getFilters().size(), 2);
  }

  // Helper methods

  private Filter buildSimpleFilter() {
    Filter filter = new Filter();
    Criterion criterion = new Criterion();
    criterion.setField("testField");
    criterion.setCondition(Condition.EQUAL);
    criterion.setValues(new com.linkedin.data.template.StringArray("testValue"));

    ConjunctiveCriterion and = new ConjunctiveCriterion();
    and.setAnd(new CriterionArray(ImmutableList.of(criterion)));
    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(and)));
    return filter;
  }

  private EntityResponse buildViewEntityResponse(String json, Filter filter) {
    DataHubViewInfo info = new DataHubViewInfo();
    info.setName(TEST_VIEW_NAME);
    info.setDescription(TEST_VIEW_DESCRIPTION);
    info.setType(DataHubViewType.PERSONAL);

    DataHubViewDefinition definition = new DataHubViewDefinition();
    definition.setEntityTypes(new com.linkedin.data.template.StringArray());
    definition.setFilter(filter);
    if (json != null) {
      definition.setJson(json);
    }
    info.setDefinition(definition);

    EntityResponse response = new EntityResponse();
    response.setUrn(UrnUtils.getUrn(TEST_VIEW_URN));
    response.setEntityName("dataHubView");

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(info.data()));
    aspectMap.put(Constants.DATAHUB_VIEW_INFO_ASPECT_NAME, envelopedAspect);
    response.setAspects(aspectMap);

    return response;
  }
}
