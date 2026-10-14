package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.query.v3.SearchTemplateMustache;
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
    assertTrue(SearchTemplateQueryShape.isIdentity("urn:li:corpuser:alice"));
    assertTrue(SearchTemplateQueryShape.isIdentity("gs://bucket/path"));
    assertFalse(SearchTemplateQueryShape.isIdentity("orders"));
    assertEquals(SearchTemplateQueryShape.of("s3://bucket/path/object").lastSegment, "object");
    assertEquals(
        SearchTemplateQueryShape.of("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)")
            .lastSegment,
        "db.table");
    assertEquals(SearchTemplateQueryShape.of("db.schema.table").fqnQuery, "db schema table");
  }

  @Test
  public void testSharedFieldWeights() {
    List<String> analyzed =
        SearchTemplateFields.analyzedFields(SearchTemplateFields.sharedDefaults());
    assertTrue(analyzed.contains("name.text^10"));
    assertTrue(analyzed.contains("qualifiedName.text^10"));
    assertFalse(analyzed.stream().anyMatch(name -> name.startsWith("name.stemmed")));
    assertFalse(analyzed.stream().anyMatch(name -> name.startsWith("qualifiedName.stemmed")));
    assertTrue(analyzed.contains("description.text^1"));
    assertTrue(analyzed.contains("description.stemmed^0.5"));
    assertTrue(analyzed.contains("columns.text^1"));
    assertTrue(analyzed.contains("columns.stemmed^0.5"));
    assertTrue(analyzed.contains("tags^0.5"));
    assertFalse(analyzed.stream().anyMatch(name -> name.contains("customProperties")));
    assertFalse(analyzed.stream().anyMatch(name -> name.contains("_search.")));
    assertEquals(SearchTemplateFields.keywordPath("name"), "name.keyword");
    assertEquals(SearchTemplateFields.keywordPath("platform"), "platform");
    assertEquals(SearchTemplateFields.keywordPath("tags_1"), "tags_1");
    assertTrue(
        SearchTemplateFields.highlightRoots(SearchTemplateFields.sharedDefaults())
            .contains("columns"));
    assertFalse(
        SearchTemplateFields.highlightRoots(SearchTemplateFields.sharedDefaults())
            .contains("fieldPaths"));
  }
}
