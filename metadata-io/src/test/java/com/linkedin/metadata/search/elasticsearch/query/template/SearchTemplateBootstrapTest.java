package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchTemplateMustache;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.List;
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

  private String render(SearchTemplateLoader loader, String urn) {
    OperationContext bound =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(urn, 1L, "fulltext", null));
    return new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
        .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
        .toString();
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
