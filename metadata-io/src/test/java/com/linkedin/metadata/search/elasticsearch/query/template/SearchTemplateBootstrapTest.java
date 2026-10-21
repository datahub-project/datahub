package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Shipped bootstrap templates fill storage, and system update ignores storage. */
public class SearchTemplateBootstrapTest {
  private static final String STRUCTURED = "urn:li:searchTemplate:structured";
  private static final String ENTRY = "urn:li:searchTemplate:searchLight";
  private static final String SEARCH = "urn:li:searchTemplate:search";
  private static final String PROPERTY = "urn:li:structuredProperty:retention";
  private static final String PROPERTY_QUERY =
      "{\"match\": {\"name\": \"{{query}} {{urn:li:structuredProperty:retention}}\"}}";

  private SearchTemplateFixtures.Harness harness;

  @BeforeMethod
  public void setup() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
  }

  @Test
  public void testBootstrapRenderKeepsJsonQuotes() {
    String rendered = SearchTemplateBootstrap.render("stamp: {{auditStamp}}\n", "fixture");

    assertTrue(rendered.contains("urn:li:corpuser:"));
    assertFalse(rendered.contains("&quot;"));
    assertFalse(rendered.contains("&amp;"));
  }

  @Test
  public void testClasspathIgnoresStorage() {
    harness.store(STRUCTURED, marker("STORED-MARKER"));
    SearchTemplateLoader loader = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    MergedSearchTemplate structured = loader.load(harness.opContext, STRUCTURED);
    MergedSearchTemplate entry = loader.load(harness.opContext, ENTRY);

    assertTrue(structured.isClasspathBridge());
    assertFalse(structured.getQuery().contains("STORED-MARKER"));
    assertFalse(structured.getQuery().contains("{{=<% %>=}}"));
    assertFalse(structured.getQuery().contains("<%={{ }}=%>"));
    assertTrue(entry.isClasspathBridge());
    assertTrue(entry.getChain().contains(SEARCH));
    assertFalse(entry.getRoutes().isEmpty());
    verify(harness.retriever, never()).getLatestAspectObject(any(), any(), any());
    expectThrows(
        SearchTemplateException.class,
        () -> loader.load(harness.opContext, "urn:li:searchTemplate:does-not-exist"));
  }

  @Test
  public void testStoragePrefersStoredAspect() {
    harness.store(STRUCTURED, marker("STORED-MARKER"));
    MergedSearchTemplate structured = harness.loader.load(harness.opContext, STRUCTURED);
    MergedSearchTemplate entry = harness.loader.load(harness.opContext, ENTRY);

    assertFalse(structured.isClasspathBridge());
    assertTrue(structured.getQuery().contains("STORED-MARKER"));
    assertTrue(entry.isClasspathBridge());
    assertTrue(entry.getChain().contains(SEARCH));
    assertFalse(entry.getRoutes().isEmpty());
  }

  @Test
  public void testClasspathDropsStructuredPropertyReference() {
    SearchTemplateProperties bridge = marker(PROPERTY_QUERY);
    SearchTemplateLoader loader =
        new SearchTemplateLoader(
            0, SearchTemplateSource.CLASSPATH, Map.of("urn:li:searchTemplate:bridge", bridge));
    String rendered = render(loader, "urn:li:searchTemplate:bridge");

    assertFalse(rendered.contains("structuredProperty"));
    assertFalse(rendered.contains("structuredProperties"));
    verify(harness.retriever, never())
        .getLatestAspectObject(any(), eq(UrnUtils.getUrn(PROPERTY)), any());
  }

  @Test
  public void testStorageResolvesStructuredPropertyReference() {
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setQualifiedName("retention");
    definition.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    definition.setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    harness.definitions.put(UrnUtils.getUrn(PROPERTY), definition);
    harness.store("urn:li:searchTemplate:bridge", marker(PROPERTY_QUERY));

    String rendered = render(harness.loader, "urn:li:searchTemplate:bridge");

    assertFalse(rendered.contains("{{urn:li:structuredProperty:retention}}"));
    assertTrue(rendered.contains("structuredProperties"));
    verify(harness.retriever)
        .getLatestAspectObject(
            any(), eq(UrnUtils.getUrn(PROPERTY)), eq(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME));
  }

  @Test
  public void testShippedTemplatesValidateAsASet() {
    SearchTemplateLoader loader = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    Set<String> urns = SearchTemplateBootstrap.urns();
    assertTrue(urns.contains(STRUCTURED));
    assertTrue(urns.contains(ENTRY));
    assertTrue(urns.contains("urn:li:searchTemplate:browseV2"));
    assertTrue(urns.contains("urn:li:searchTemplate:browseV2List"));
    assertTrue(urns.contains("urn:li:searchTemplate:browseV2Fulltext"));
    assertNull(
        loader.load(harness.opContext, "urn:li:searchTemplate:browseV2List").getFunctionScore());
    assertNull(
        loader
            .load(harness.opContext, "urn:li:searchTemplate:browseV2Fulltext")
            .getFunctionScore());
    for (String urn : urns) {
      SearchTemplateProperties shipped = SearchTemplateBootstrap.get(urn);
      SearchTemplateChecks.check(shipped, 1024);
      SearchTemplateChecks.checkMerged(loader.load(harness.opContext, urn), 1024);
    }

    SearchTemplateProperties fulltext =
        SearchTemplateBootstrap.get("urn:li:searchTemplate:fulltext");
    Map<String, Object> context =
        SearchTemplateMustache.validationContext(fulltext.getFunctionScore());
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fieldFunctions =
        (List<Map<String, Object>>) context.get("fieldScoreFunctions");
    assertEquals(fieldFunctions.size(), 1);
    assertEquals(fieldFunctions.get(0).get("modifier"), "log1p");
    assertFalse(context.containsKey("modifier"));

    SearchTemplateProperties nested =
        marker(
            """
            {
              "bool": {
                "should": [
                  {{#fieldScoreFunctions}}
                  {{#notARealSection}}
                  { "match_all": {} }
                  {{/notARealSection}}
                  {{/fieldScoreFunctions}}
                ]
              }
            }
            """);
    SearchTemplateException error =
        expectThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(nested, 1024));
    assertTrue(error.getMessage().contains("notARealSection"));
    assertFalse(error.getMessage().contains("leaf clauses"));
  }

  /** A quoted phrase of four or more tokens matches a description that contains every term. */
  @Test
  public void testQuotedLongPhraseMatchesDescription() throws Exception {
    SearchTemplateLoader loader = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    JsonNode phrase =
        SearchTemplateFixtures.JSON.readTree(
            render(
                loader, "urn:li:searchTemplate:exact", "\"basic information about a customer\""));
    JsonNode description = descriptionMatch(phrase);
    assertEquals(description.path("query").asText(), "basic information about a customer");
    assertEquals(description.path("operator").asText().toLowerCase(Locale.ROOT), "and");
    assertEquals(description.path("boost").asDouble(), 100.0d);

    JsonNode name =
        SearchTemplateFixtures.JSON.readTree(
            render(loader, "urn:li:searchTemplate:exact", "\"pet_profiles\""));
    assertFalse(name.toString().contains("description"));
  }

  /** A compound synonym requires every word. The original one-token query stays lenient. */
  @Test
  public void testCompoundSynonymRequiresEveryToken() throws Exception {
    SearchTemplateLoader loader = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    JsonNode fulltext =
        SearchTemplateFixtures.JSON.readTree(
            render(loader, "urn:li:searchTemplate:fulltext", "dataPlatform"));
    assertClause(fulltext, "simple_query_string", "data platform", "default_operator", "and");
    assertClause(fulltext, "simple_query_string", "dataPlatform~2", "default_operator", "or");

    JsonNode light =
        SearchTemplateFixtures.JSON.readTree(
            render(loader, "urn:li:searchTemplate:light", "dataPlatform"));
    assertClause(light, "multi_match", "data platform", "operator", "and");
    assertClause(light, "multi_match", "dataPlatform", "operator", "or");
  }

  private String render(SearchTemplateLoader loader, String urn) {
    return render(loader, urn, "orders");
  }

  private String render(SearchTemplateLoader loader, String urn, String query) {
    OperationContext bound =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(urn, 1L, "fulltext", null));
    return new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
        .render(bound, List.of(TestEntitySpecBuilder.getSpec()), query, true)
        .toString();
  }

  private static JsonNode descriptionMatch(JsonNode node) {
    for (JsonNode match : node.findValues("match")) {
      var fields = match.fields();
      while (fields.hasNext()) {
        var field = fields.next();
        if (field.getKey().contains("description")) {
          return field.getValue();
        }
      }
    }
    throw new AssertionError(node.toString());
  }

  private static void assertClause(
      JsonNode node, String clause, String query, String operatorField, String operator) {
    List<JsonNode> matches = new ArrayList<>();
    collectClauses(node, clause, query, matches);
    assertEquals(matches.size(), 1, clause + " " + query);
    assertEquals(matches.get(0).path(operatorField).asText().toLowerCase(Locale.ROOT), operator);
  }

  private static void collectClauses(
      JsonNode node, String clause, String query, List<JsonNode> matches) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      JsonNode body = node.get(clause);
      if (body != null && query.equals(body.path("query").asText())) {
        matches.add(body);
      }
      node.fields()
          .forEachRemaining(entry -> collectClauses(entry.getValue(), clause, query, matches));
    } else if (node.isArray()) {
      node.forEach(child -> collectClauses(child, clause, query, matches));
    }
  }

  private static SearchTemplateProperties marker(String query) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName("Marker");
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(false);
    properties.setQuery(query);
    return properties;
  }
}
