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
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateSearchType;
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
  private static final String FULLTEXT = "urn:li:searchTemplate:fulltext";
  private static final String LIGHT = "urn:li:searchTemplate:light";
  private static final String SEMANTIC = "urn:li:searchTemplate:semantic";
  private static final String SIMILARITY = "urn:li:searchTemplate:similarity";
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
  public void testShippedListSkipsDfsAndTextTemplatesLeaveItUnset() {
    SearchTemplateLoader loader = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    assertEquals(
        loader.load(harness.opContext, "urn:li:searchTemplate:list").getSearchType(),
        SearchTemplateSearchType.QUERY_THEN_FETCH);
    assertEquals(
        loader.load(harness.opContext, "urn:li:searchTemplate:listUrn").getSearchType(),
        SearchTemplateSearchType.QUERY_THEN_FETCH);
    assertNull(loader.load(harness.opContext, "urn:li:searchTemplate:fulltext").getSearchType());
    assertNull(loader.load(harness.opContext, "urn:li:searchTemplate:light").getSearchType());
    assertNull(loader.load(harness.opContext, "urn:li:searchTemplate:exact").getSearchType());
    assertNull(loader.load(harness.opContext, "urn:li:searchTemplate:identity").getSearchType());
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
    assertFalse(urns.contains("urn:li:searchTemplate:lineage"));
    assertFalse(urns.contains("urn:li:searchTemplate:browseV2"));
    assertFalse(urns.contains("urn:li:searchTemplate:browseV2List"));
    assertFalse(urns.contains("urn:li:searchTemplate:browseV2Fulltext"));
    assertTrue(urns.contains("urn:li:searchTemplate:listUrn"));
    for (String urn : urns) {
      SearchTemplateProperties shipped = SearchTemplateBootstrap.get(urn);
      SearchTemplateChecks.check(shipped, 1024);
      SearchTemplateChecks.checkMerged(loader.load(harness.opContext, urn), 1024);
    }

    SearchTemplateProperties fulltext =
        SearchTemplateBootstrap.get("urn:li:searchTemplate:fulltext");
    MergedSearchTemplate list = loader.load(harness.opContext, "urn:li:searchTemplate:list");
    MergedSearchTemplate listUrn = loader.load(harness.opContext, "urn:li:searchTemplate:listUrn");
    MergedSearchTemplate autocomplete =
        loader.load(harness.opContext, "urn:li:searchTemplate:autocomplete");
    assertNull(list.getEntityTypeBoosts());
    assertNull(listUrn.getEntityTypeBoosts());
    assertNull(autocomplete.getEntityTypeBoosts());
    assertNull(listUrn.getFunctionScore());
    assertNull(listUrn.getBoosts());
    assertTrue(fulltext.hasEntityTypeBoosts());
    assertTrue(listUrn.getSort().contains("\"urn\""));
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

  /**
   * fulltext reranks with the default blend and no floor, and light inherits that rerank rather
   * than restating it. semantic is one vector window with no cap, floor, next, or route yet.
   * similarity is the anchor's neighbors, 500 at most, above a cosine of 0.8, without the anchor
   * itself. None selects an embedding profile or an entity scope, so each entity type resolves to
   * its registry default.
   */
  @Test
  public void testShippedKeywordTemplatesRerankAndSemanticRetrievesByVector() {
    SearchTemplateLoader loader = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    MergedSearchTemplate fulltext = loader.load(harness.opContext, FULLTEXT);
    MergedSearchTemplate light = loader.load(harness.opContext, LIGHT);
    MergedSearchTemplate semantic = loader.load(harness.opContext, SEMANTIC);
    MergedSearchTemplate similarity = loader.load(harness.opContext, SIMILARITY);

    assertTrue(SearchTemplateCapabilities.keywordRerank(fulltext));
    SearchTemplateRerankKnn blend = SearchTemplateJson.parseRerankKnn(fulltext.getKnn());
    assertEquals(blend.others(), SearchTemplateRerankKnn.KEEP_KEYWORD_PLACES);
    assertNull(blend.blendFloor());
    assertNull(blend.model());
    assertNull(blend.field());
    assertEquals(blend.vectorWeight(), SearchTemplateRerankKnn.DEFAULT_VECTOR_WEIGHT);
    assertTrue(SearchTemplateCapabilities.keywordRerank(light));
    assertEquals(light.getRetrieval(), fulltext.getRetrieval());
    assertEquals(light.getKnn(), fulltext.getKnn());
    SearchTemplateProperties storedLight = SearchTemplateBootstrap.get(LIGHT);
    assertFalse(storedLight.hasRetrieval());
    assertFalse(storedLight.hasKnn());

    assertTrue(SearchTemplateCapabilities.vectorRetrieval(semantic));
    SearchTemplateRetrieval vector = semantic.getRetrieval();
    assertNull(vector.getCandidateCap());
    assertFalse(vector.hasMinSimilarity());
    assertNull(vector.getOnFailure());
    assertNull(semantic.getKnn());
    assertNull(semantic.getLadder());
    assertTrue(semantic.getRoutes().isEmpty());

    assertTrue(SearchTemplateCapabilities.vectorRetrieval(similarity));
    SearchTemplateRetrieval anchored = similarity.getRetrieval();
    assertEquals(anchored.getQuerySource(), SearchTemplateCapabilities.QUERY_SOURCE_ANCHOR_ENTITY);
    assertEquals(anchored.getCandidateCap().intValue(), 500);
    assertEquals(anchored.getMinSimilarity(), 0.8f);
    assertEquals(anchored.getAnchor().getUrnParameter(), "raw");
    assertTrue(anchored.getAnchor().isExcludeSelf());
    assertNull(similarity.getKnn());
    assertNull(similarity.getLadder());
    assertTrue(similarity.getRoutes().isEmpty());

    for (MergedSearchTemplate template : List.of(fulltext, light, semantic, similarity)) {
      assertNull(template.getProfiles(), template.getUrn());
      assertFalse(template.isProfilesSet(), template.getUrn());
      assertNull(template.getScope(), template.getUrn());
      assertNull(SearchTemplateGates.legacyProfileId(template), template.getUrn());
    }
  }

  /**
   * The revert seed writes fulltext back without its keywordRerank sections, as the previous seed
   * wrote it. Over the shipped templates, every template an earlier release can select then
   * resolves to contract 1 with no retrieval or knn. semantic and similarity stay: no earlier
   * release selects them.
   */
  @Test
  public void testRevertSeedRestoresTheKeywordOnlyTemplates() {
    Map<String, SearchTemplateProperties> revert =
        SearchTemplateFixtures.readSeed("/bootstrap_mcps/search-templates-revert.yaml");
    assertEquals(revert.keySet(), Set.of(FULLTEXT));
    SearchTemplateProperties keywordOnly = SearchTemplateFixtures.readShipped().get(FULLTEXT);
    keywordOnly.removeRetrieval();
    keywordOnly.removeKnn();
    keywordOnly.setApiVersion(SearchTemplateCapabilities.API_VERSION);
    assertEquals(revert.get(FULLTEXT), keywordOnly);
    SearchTemplateChecks.check(revert.get(FULLTEXT), 1024);

    SearchTemplateFixtures.readShipped().forEach(harness::store);
    revert.forEach(harness::store);
    for (String urn : SearchTemplateBootstrap.urns()) {
      if (SEMANTIC.equals(urn) || SIMILARITY.equals(urn)) {
        continue;
      }
      MergedSearchTemplate merged = harness.loader.load(harness.opContext, urn);
      assertEquals(merged.getApiVersion(), SearchTemplateCapabilities.API_VERSION, urn);
      assertNull(merged.getRetrieval(), urn);
      assertNull(merged.getKnn(), urn);
    }
  }

  /**
   * A custom child of fulltext that sets its own knn and no retrieval keeps the reading that knn
   * had before: a knn with only a field is the legacy document rerank. A child that leaves knn
   * unset inherits keywordRerank.
   */
  @Test
  public void testChildKnnKeepsItsLegacyReadingUnderTheShippedRerank() {
    SearchTemplateProperties legacy = child("Legacy");
    legacy.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    harness.store("urn:li:searchTemplate:legacyChild", legacy);
    harness.store("urn:li:searchTemplate:plainChild", child("Plain"));

    MergedSearchTemplate legacyMerged =
        harness.loader.load(harness.opContext, "urn:li:searchTemplate:legacyChild");
    assertNull(SearchTemplateCapabilities.effectiveCombine(legacyMerged));
    assertEquals(legacyMerged.getKnn(), legacy.getKnn());
    assertFalse(legacyMerged.isKnnInherited());
    MergedSearchTemplate plainMerged =
        harness.loader.load(harness.opContext, "urn:li:searchTemplate:plainChild");
    assertTrue(SearchTemplateCapabilities.keywordRerank(plainMerged));
    assertTrue(plainMerged.isKnnInherited());
  }

  /**
   * A child that sets its own keywordRerank combine, as the revert preflight asks, keeps the blend
   * its parent chain sets, so its rerank does not fall back to the default blend.
   */
  @Test
  public void testOwnKeywordRerankKeepsTheInheritedBlend() {
    SearchTemplateProperties tuned = child("Tuned");
    tuned.setKnn("{ \"vectorWeight\": 0.3, \"others\": \"keepKeywordPlaces\" }");
    harness.store("urn:li:searchTemplate:tunedChild", tuned);
    SearchTemplateProperties restated = child("Restated");
    restated.setParent(UrnUtils.getUrn("urn:li:searchTemplate:tunedChild"));
    restated.setApiVersion(SearchTemplateCapabilities.SEMANTIC_API_VERSION);
    restated.setRetrieval(
        new SearchTemplateRetrieval()
            .setCombine(SearchTemplateCapabilities.COMBINE_KEYWORD_RERANK));
    harness.store("urn:li:searchTemplate:restatedChild", restated);

    MergedSearchTemplate merged =
        harness.loader.load(harness.opContext, "urn:li:searchTemplate:restatedChild");
    assertTrue(SearchTemplateCapabilities.keywordRerank(merged));
    assertEquals(merged.getKnn(), tuned.getKnn());
    assertTrue(merged.isKnnInherited());
  }

  /**
   * A child's own retrieval drops only an inherited blend without a field. Under a parent whose knn
   * names a field, a child that sets its own retrieval keeps that knn, the legacy document rerank.
   */
  @Test
  public void testOwnRetrievalKeepsAnInheritedLegacyFieldKnn() {
    SearchTemplateProperties legacy = child("Legacy");
    legacy.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    harness.store("urn:li:searchTemplate:legacyChild", legacy);
    SearchTemplateProperties keyword = child("Keyword");
    keyword.setParent(UrnUtils.getUrn("urn:li:searchTemplate:legacyChild"));
    keyword.setRetrieval(new SearchTemplateRetrieval().setMode("keyword"));
    harness.store("urn:li:searchTemplate:keywordGrandchild", keyword);

    MergedSearchTemplate merged =
        harness.loader.load(harness.opContext, "urn:li:searchTemplate:keywordGrandchild");
    assertEquals(merged.getKnn(), legacy.getKnn());
    assertTrue(SearchTemplateCapabilities.legacyKnn(merged));
  }

  /**
   * A child's own legacy field knn below a retrieval that sets keywordRerank drops only that
   * combine. The rest of the retrieval, here its keyword mode, still applies to the child.
   */
  @Test
  public void testOwnLegacyKnnDropsOnlyTheInheritedCombine() {
    SearchTemplateProperties keyword = child("Keyword");
    keyword.setApiVersion(SearchTemplateCapabilities.SEMANTIC_API_VERSION);
    keyword.setRetrieval(
        new SearchTemplateRetrieval()
            .setMode("keyword")
            .setCombine(SearchTemplateCapabilities.COMBINE_KEYWORD_RERANK));
    harness.store("urn:li:searchTemplate:keywordChild", keyword);
    SearchTemplateProperties legacy = child("Legacy");
    legacy.setParent(UrnUtils.getUrn("urn:li:searchTemplate:keywordChild"));
    legacy.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    harness.store("urn:li:searchTemplate:legacyGrandchild", legacy);

    MergedSearchTemplate merged =
        harness.loader.load(harness.opContext, "urn:li:searchTemplate:legacyGrandchild");
    assertEquals(merged.getRetrieval(), new SearchTemplateRetrieval().setMode("keyword"));
    assertEquals(merged.getKnn(), legacy.getKnn());
    assertTrue(SearchTemplateCapabilities.legacyKnn(merged));
  }

  private static SearchTemplateProperties child(String name) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(name);
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    properties.setParent(UrnUtils.getUrn(FULLTEXT));
    return properties;
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

  /**
   * A compound synonym requires every word. The original one-token query stays lenient. Fulltext
   * repeats the synonym on the shared fields, name, and qualifiedName. Id and email stay literal. A
   * single word skips the shared fields on the light query and keeps name and qualifiedName. The
   * light query itself also matches id and email.
   */
  @Test
  public void testCompoundSynonymRequiresEveryToken() throws Exception {
    SearchTemplateLoader loader = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    JsonNode fulltext =
        SearchTemplateFixtures.JSON.readTree(
            render(loader, "urn:li:searchTemplate:fulltext", "dataPlatform"));
    assertClause(fulltext, "simple_query_string", "data platform", "default_operator", "and", 3);
    assertClause(fulltext, "simple_query_string", "dataPlatform~2", "default_operator", "or", 4);

    JsonNode light =
        SearchTemplateFixtures.JSON.readTree(
            render(loader, "urn:li:searchTemplate:light", "dataPlatform"));
    assertClause(light, "multi_match", "data platform", "operator", "and", 2);
    assertClause(light, "multi_match", "dataPlatform", "operator", "or", 4);
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
      JsonNode node,
      String clause,
      String query,
      String operatorField,
      String operator,
      int expected) {
    List<JsonNode> matches = new ArrayList<>();
    collectClauses(node, clause, query, matches);
    assertEquals(matches.size(), expected, clause + " " + query);
    for (JsonNode match : matches) {
      assertEquals(match.path(operatorField).asText().toLowerCase(Locale.ROOT), operator);
    }
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
