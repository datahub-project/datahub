package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class SearchTemplateSynonymTest {

  @Test
  public void testEquivalenceExpansionsSkipExplicitRules() {
    assertEquals(SearchTemplateSynonyms.expansions("prod"), List.of("production"));
    assertEquals(SearchTemplateSynonyms.expansions("production"), List.of("prod"));
    assertTrue(SearchTemplateSynonyms.expansions("cac").isEmpty());
    assertFalse(SearchTemplateSynonyms.expansions("cac").contains("customer"));
    assertEquals(SearchTemplateSynonyms.expansions("data platform"), List.of("dataplatform"));
    assertEquals(SearchTemplateSynonyms.expansions(""), List.of());
    assertEquals(SearchTemplateSynonyms.expansions("glue"), List.of("athena"));
  }

  @Test
  public void testEmptySynonymListRendersNoClauses() {
    String template = "{{#synonymQueries}}{{.}}{{/synonymQueries}}";
    Map<String, Object> prod = new LinkedHashMap<>();
    prod.put("synonymQueries", SearchTemplateSynonyms.expansions("prod"));
    assertEquals(SearchTemplateMustache.render(template, prod), "production");

    Map<String, Object> cac = new LinkedHashMap<>();
    cac.put("synonymQueries", SearchTemplateSynonyms.expansions("cac"));
    assertEquals(SearchTemplateMustache.render(template, cac), "");

    Map<String, Object> empty = new LinkedHashMap<>();
    empty.put("synonymQueries", List.of());
    assertEquals(SearchTemplateMustache.render(template, empty), "");
  }

  @Test
  public void testQueryShape() {
    SearchTemplateQueryShape orders = SearchTemplateQueryShape.of("orders2017");
    assertEquals(orders.normalized, "orders 2017");
    assertEquals(orders.wholeIdentifier, "orders2017");
    assertEquals(orders.letterDigitQuery, "orders 2017");
    SearchTemplateQueryShape job = SearchTemplateQueryShape.of("load_job-0001");
    assertEquals(job.wholeIdentifier, "load_job-0001");
    assertEquals(job.letterDigitQuery, null);
    assertEquals(SearchTemplateQueryShape.of("orders").fuzzyQuery, "orders~1");
    assertEquals(SearchTemplateQueryShape.of("name").fuzzyQuery, "name");
    assertEquals(SearchTemplateQueryShape.of("qualified").fuzzyQuery, "qualified~2");
    assertEquals(SearchTemplateQueryShape.of("orders").wildcardTerm, "orders");
    assertEquals(SearchTemplateQueryShape.of("orders2017").wildcardTerm, "orders2017");
    assertEquals(SearchTemplateQueryShape.of("name").wildcardTerm, null);
    assertTrue(SearchTemplateQueryShape.of("db.schema.table").nameOnly);
    assertFalse(SearchTemplateQueryShape.of("hello db.table").nameOnly);
    assertTrue(SearchTemplateQueryShape.stopOnEmptyLight("1234567"));
    assertTrue(SearchTemplateQueryShape.stopOnEmptyLight("a.b.c.d"));
    assertFalse(SearchTemplateQueryShape.stopOnEmptyLight("orders"));
    assertFalse(SearchTemplateQueryShape.stopOnEmptyLight("orders 2017"));
    assertTrue(SearchTemplateQueryShape.of("orders2017").exactNameLetterDigit());
    assertTrue(SearchTemplateQueryShape.of("cargo2017").exactNameLetterDigit());
    assertFalse(SearchTemplateQueryShape.of("sales orders2017").exactNameLetterDigit());
    assertTrue(SearchTemplateQueryShape.of("fleet_v2").exactNameLetterDigit());
    assertFalse(SearchTemplateQueryShape.of("orders2017!").exactNameLetterDigit());
    assertTrue(SearchTemplateQueryShape.isIdentity("urn:li:corpuser:alice"));
    assertTrue(
        SearchTemplateQueryShape.isIdentity("urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)"));
    assertTrue(SearchTemplateQueryShape.isIdentity("gs://bucket/path"));
    assertTrue(SearchTemplateQueryShape.isIdentity("s3:\\/\\/bucket\\/key"));
    assertTrue(SearchTemplateQueryShape.isIdentity("hdfs:\\/\\/warehouse\\/events"));
    assertFalse(SearchTemplateQueryShape.isIdentity("orders"));
    assertFalse(SearchTemplateQueryShape.isIdentity("urn:orders"));
    assertFalse(SearchTemplateQueryShape.isIdentity("URN:orders"));
    assertFalse(SearchTemplateQueryShape.isIdentity("urn:"));
    assertFalse(SearchTemplateQueryShape.isIdentity("urn:li:"));
    assertEquals(SearchTemplateQueryShape.of("s3://bucket/path/object").lastSegment, "object");
    assertEquals(
        SearchTemplateQueryShape.of("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)")
            .lastSegment,
        "db.table");
    assertEquals(SearchTemplateQueryShape.of("db.schema.table").fqnQuery, "db schema table");
  }

  @Test
  public void testLogicalRootOverridesRespectDegree() {
    List<SearchFieldConfig> base =
        List.of(
            field("name.text", 10),
            field("name.stemmed", 5),
            field("name_1.text", 4),
            field("name_1.stemmed", 2));
    SearchFieldOverrides remove = new SearchFieldOverrides();
    remove.setRemove(new StringArray("name"));
    List<SearchFieldConfig> withoutRoot = SearchTemplateFields.applyTemplate(base, remove);
    assertEquals(
        withoutRoot.stream().map(SearchFieldConfig::fieldName).toList(),
        List.of("name_1.text", "name_1.stemmed"));

    SearchFieldBoost degreeOne = new SearchFieldBoost().setFieldName("name_1").setBoost(7.0);
    SearchFieldOverrides override = new SearchFieldOverrides();
    override.setOverride(new SearchFieldBoostArray(degreeOne));
    List<SearchFieldConfig> overridden = SearchTemplateFields.applyTemplate(base, override);
    assertEquals(
        overridden.stream()
            .filter(field -> field.fieldName().startsWith("name_1."))
            .map(SearchFieldConfig::boost)
            .toList(),
        List.of(7.0f, 7.0f));
    assertEquals(overridden.get(0).boost(), 10.0f);
  }

  private static SearchFieldConfig field(String name, float boost) {
    return SearchFieldConfig.detectSubFieldType(
        name, boost, SearchableAnnotation.FieldType.TEXT, true);
  }
}
