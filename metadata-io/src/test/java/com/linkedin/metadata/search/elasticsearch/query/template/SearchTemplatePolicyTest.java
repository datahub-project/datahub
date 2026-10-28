package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.SearchClauseBudget;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateNormalization;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.StructuredPropertyClause;
import com.linkedin.search.template.StructuredPropertyOperator;
import com.linkedin.search.template.StructuredPropertyQuery;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Template-owned property clauses, /q routing, normalization, and unused custom search
 * configuration.
 */
public class SearchTemplatePolicyTest {
  private static final String PARENT = "urn:li:searchTemplate:policyParent";
  private static final String CHILD = "urn:li:searchTemplate:policyChild";

  private SearchTemplateFixtures.Harness harness;
  private EntitySpec dataset;

  @BeforeMethod
  public void setup() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    harness.installCatalog();
    dataset = harness.registry.getEntitySpec("dataset");
  }

  @Test
  public void testPropertyClausesFollowTheTemplate() {
    String path = harness.property("certification.status").fieldPath();
    SearchTemplateProperties exactOnly = body("{ \"match_all\": {} }");
    StructuredPropertyQuery exact = new StructuredPropertyQuery();
    exact.setExactValue(new StructuredPropertyClause());
    StructuredPropertyClause analyzedOff = new StructuredPropertyClause();
    analyzedOff.setEnabled(false);
    exact.setAnalyzedValue(analyzedOff);
    exactOnly.setStructuredPropertyQuery(exact);
    harness.store(PARENT, exactOnly);
    String exactQuery = render(PARENT, "gold");
    assertTrue(exactQuery.contains(path), exactQuery);
    assertFalse(exactQuery.contains(path + ".keyword"), exactQuery);
    assertFalse(exactQuery.contains("simple_query_string"), exactQuery);

    SearchTemplateProperties analyzed = body("{ \"match_all\": {} }");
    StructuredPropertyClause and = new StructuredPropertyClause();
    and.setOperator(StructuredPropertyOperator.AND);
    and.setWeight(2f);
    StructuredPropertyQuery section = new StructuredPropertyQuery();
    section.setAnalyzedValue(and);
    analyzed.setStructuredPropertyQuery(section);
    harness.store(CHILD, analyzed);
    String analyzedQuery = render(CHILD, "gold");
    assertTrue(analyzedQuery.contains("\"default_operator\" : \"and\""), analyzedQuery);
    assertTrue(analyzedQuery.contains(path + ".text"), analyzedQuery);
    assertFalse(analyzedQuery.contains(path + ".keyword"), analyzedQuery);
  }

  @Test
  public void testStructuredFragmentMatchesRoutingAndOmitsSystemFilters() {
    harness.installShippedTemplates();
    SearchTemplateProperties shipped =
        SearchTemplateBootstrap.get("urn:li:searchTemplate:structured");
    assertTrue(shipped.getQuery().contains("{{&structuredQuery}}"));
    assertFalse(shipped.getQuery().contains("\"query\": \"{{raw}}\""));

    String named = render("urn:li:searchTemplate:structured", "name:orders");
    assertTrue(
        queryStrings(named).stream()
            .noneMatch(clause -> "name:orders".equals(clause.path("query").asText())),
        named);
    assertTrue(
        terms(named, "name.lowercase").stream()
            .anyMatch(
                term ->
                    "orders".equals(term.path("value").asText())
                        && !term.path("case_insensitive").asBoolean(false)),
        named);
    assertFalse(named.contains("\"removed\""), named);

    String property =
        render(
            "urn:li:searchTemplate:structured", "structuredProperties.certification_status:gold");
    List<JsonNode> propertyClauses = queryStrings(property);
    assertEquals(propertyClauses.size(), 1, property);
    assertEquals(
        propertyClauses.get(0).get("query").asText(),
        "structuredProperties.certification_status:gold");
    assertTrue(propertyClauses.get(0).path("lenient").asBoolean(false), property);
  }

  @Test
  public void testNormalizationInheritsAndBudgetClamps() {
    SearchTemplateProperties parent = body("{ \"match_all\": {} }");
    SearchTemplateNormalization parentLimits = new SearchTemplateNormalization();
    parentLimits.setWildcardMinimumLength(4);
    parentLimits.setSynonymCap(8);
    parentLimits.setFuzzyTokenThreshold(5);
    parent.setNormalization(parentLimits);
    harness.store(PARENT, parent);

    SearchTemplateProperties child = body("{ \"match_all\": {} }");
    child.setParent(UrnUtils.getUrn(PARENT));
    SearchTemplateNormalization childLimits = new SearchTemplateNormalization();
    childLimits.setSynonymCap(2);
    child.setNormalization(childLimits);
    harness.store(CHILD, child);

    MergedSearchTemplate inherited = harness.loader.load(harness.opContext, CHILD);
    assertEquals(inherited.getNormalization().getWildcardMinimumLength().intValue(), 4);
    assertEquals(inherited.getNormalization().getSynonymCap().intValue(), 2);
    assertEquals(inherited.getNormalization().getFuzzyTokenThreshold().intValue(), 5);

    child.setNormalization(new SearchTemplateNormalization());
    harness.expire();
    assertNull(harness.loader.load(harness.opContext, CHILD).getNormalization());

    SearchTemplateNormalization tooMany = new SearchTemplateNormalization();
    tooMany.setSynonymCap(9);
    parent.setNormalization(tooMany);
    expectThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(parent, 1024));

    SearchClauseBudget.FieldCounts wide = new SearchClauseBudget.FieldCounts(400, 0);
    SearchTemplateNormalizationPolicy.Resolved clamped =
        SearchTemplateNormalizationPolicy.budgetClamp(
            SearchTemplateNormalizationPolicy.resolve(parentLimits), "alpha beta", wide);
    assertTrue(clamped.synonymCap() < 8, String.valueOf(clamped.synonymCap()));
    assertEquals(clamped.broadQueryTokenCap(), 2);
  }

  @Test
  public void testNormalizationChangesRenderedQueryText() {
    SearchTemplateProperties properties =
        body(
            """
            {
              "match": {
                "name": {
                  "query": "F={{fuzzy}} B={{broadQuery}} S={{#synonyms}}[{{.}}]{{/synonyms}} W={{#wildcardTerm}}YES{{/wildcardTerm}}"
                }
              }
            }
            """);
    SearchTemplateNormalization limits = new SearchTemplateNormalization();
    limits.setWildcardMinimumLength(8);
    limits.setSynonymCap(1);
    limits.setFuzzyTokenThreshold(5);
    limits.setMediumFuzzyTokenThreshold(6);
    limits.setShortFuzzyEdits(0);
    limits.setMediumFuzzyEdits(1);
    limits.setBroadQueryTokenCap(1);
    properties.setNormalization(limits);
    harness.store(PARENT, properties);

    String query = "prod glue alpha";
    String rendered = render(PARENT, query);
    assertTrue(rendered.contains("F=prod glue alpha"), rendered);
    assertFalse(rendered.contains("~2"), rendered);
    assertTrue(rendered.contains("B=prod"), rendered);
    assertFalse(rendered.contains("B=prod glue"), rendered);
    List<String> synonyms = SearchTemplateSynonyms.expansions(query);
    assertTrue(rendered.contains("[" + synonyms.get(0) + "]"), rendered);
    for (String later : synonyms.subList(1, synonyms.size())) {
      assertFalse(rendered.contains("[" + later + "]"), rendered);
    }
    assertFalse(rendered.contains("W=YES"), rendered);

    String medium = render(PARENT, "orders");
    assertTrue(medium.contains("orders~1"), medium);
    assertFalse(medium.contains("W=YES"), medium);
    String shortToken = render(PARENT, "namex");
    assertFalse(shortToken.contains("namex~"), shortToken);
  }

  @Test
  public void testDeprecatedCustomSearchFlagsStayUnused() {
    harness.installShippedTemplates();
    CustomSearchConfiguration custom =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "narrow",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().remove(List.of("description")).build())
                        .build()))
            .queryConfigurations(
                List.of(
                    QueryConfiguration.builder()
                        .queryRegex(".*")
                        .functionScore(Map.of("boost_mode", "replace"))
                        .build()))
            .build();
    OperationContext flagged =
        harness
            .opContext
            .withSearchFlags(flags -> flags.setFulltext(true).setFieldConfiguration("narrow"))
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate("urn:li:searchTemplate:fulltext", 1L, "fulltext", null));
    String withFlags =
        new SearchV3QueryBuilder(harness.loader, new SearchConfiguration(), custom)
            .render(flagged, List.of(dataset), "orders", true)
            .toString();
    OperationContext plainContext =
        harness
            .opContext
            .withSearchFlags(flags -> flags.setFulltext(true))
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate("urn:li:searchTemplate:fulltext", 1L, "fulltext", null));
    String plain =
        new SearchV3QueryBuilder(harness.loader, new SearchConfiguration(), null)
            .render(plainContext, List.of(dataset), "orders", true)
            .toString();
    assertEquals(withFlags, plain);
  }

  private String render(String urn, String query) {
    OperationContext bound =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(urn, 1L, "fulltext", null));
    return new SearchV3QueryBuilder(harness.loader, new SearchConfiguration(), null)
        .render(bound, List.of(dataset), query, true)
        .toString();
  }

  private static SearchTemplateProperties body(String query) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName("Policy");
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    properties.setQuery(query);
    return properties;
  }

  private static List<JsonNode> queryStrings(String json) {
    try {
      List<JsonNode> found = new ArrayList<>();
      collectQueryStrings(SearchTemplateFixtures.JSON.readTree(json), found);
      return found;
    } catch (Exception e) {
      throw new IllegalStateException(json, e);
    }
  }

  private static List<JsonNode> terms(String json, String field) {
    try {
      List<JsonNode> found = new ArrayList<>();
      collectTerms(SearchTemplateFixtures.JSON.readTree(json), field, found);
      return found;
    } catch (Exception e) {
      throw new IllegalStateException(json, e);
    }
  }

  private static void collectTerms(JsonNode node, String field, List<JsonNode> found) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      JsonNode term = node.get("term");
      if (term != null && term.has(field)) {
        found.add(term.get(field));
      }
      node.fields().forEachRemaining(entry -> collectTerms(entry.getValue(), field, found));
    } else if (node.isArray()) {
      node.forEach(child -> collectTerms(child, field, found));
    }
  }

  private static void collectQueryStrings(JsonNode node, List<JsonNode> found) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      JsonNode queryString = node.get("query_string");
      if (queryString != null) {
        found.add(queryString);
      }
      node.fields().forEachRemaining(entry -> collectQueryStrings(entry.getValue(), found));
    } else if (node.isArray()) {
      node.forEach(child -> collectQueryStrings(child, found));
    }
  }
}
