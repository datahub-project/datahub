package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchTemplateMatchReport;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.v3.StructuredPropertyQueryFields;
import com.linkedin.search.template.HighlightFieldOverrides;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateBoost;
import com.linkedin.search.template.SearchTemplateBoostArray;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateLadderStop;
import com.linkedin.search.template.SearchTemplateLadderStopArray;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.StructuredPropertyQuery;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Section behavior that does not depend on a schema golden. */
public class SearchTemplateRenderTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String HOLE_QUERY =
      """
      {
        "dis_max": {
          "queries": [
            { "match_all": {} }
            {{#extraDisMaxQueries}}
            ,{{&extraDisMaxQueries}}
            {{/extraDisMaxQueries}}
          ]
        }
      }
      """;

  private SearchTemplateFixtures.Harness harness;
  private EntitySpec dataset;

  @BeforeMethod
  public void setup() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    harness.installCatalog();
    dataset = harness.registry.getEntitySpec("dataset");
    SearchTemplates.install(new SearchTemplateCoordinator(harness.elastic(), null, harness.loader));
  }

  @AfterMethod
  public void clear() {
    StructuredPropertyQueryFields.replace(List.of());
    SearchTemplates.install(null);
  }

  @Test
  public void testUnsetQueryIsMatchAllAndParametersSubstitute() {
    SearchTemplateProperties unset = template("Unset", null);
    harness.store("urn:li:searchTemplate:unset", unset);
    assertTrue(render("urn:li:searchTemplate:unset", "orders", false).contains("match_all"));

    SearchTemplateProperties scored = template("Boost", null);
    scored.setQuery(
        "{ \"term\": { \"urn\": { \"value\": \"abc\", \"boost\": {{deprecatedBoost}} } } }");
    scored.setParameters(new StringMap(Map.of("deprecatedBoost", "2.5")));
    harness.store("urn:li:searchTemplate:boost", scored);
    assertTrue(render("urn:li:searchTemplate:boost", "orders", false).contains("\"boost\" : 2.5"));
  }

  @Test
  public void testClausesAndPropertyTermsSitInsideFunctionScore() throws Exception {
    SearchTemplateProperties properties = template("Clauses", "{ \"match_all\": {} }");
    properties.setClauses(
        """
        {
          "must": [ { "term": { "platform": "from-must" } } ],
          "filter": [ { "term": { "removed": false } } ],
          "should": [ { "term": { "env": "from-should" } } ],
          "must_not": [ { "term": { "status": "from-must-not" } } ]
        }
        """);
    properties.setFunctionScore(
        "{ \"functions\": [], \"score_mode\": \"multiply\", \"boost_mode\": \"multiply\" }");
    harness.store("urn:li:searchTemplate:clauses", properties);
    JsonNode root = MAPPER.readTree(render("urn:li:searchTemplate:clauses", "orders", true));
    assertTrue(root.has("function_score"));
    assertEquals(root.size(), 1);
    JsonNode bool = root.get("function_score").get("query").get("bool");
    assertNotNull(bool);
    String must = bool.get("must").toString();
    String should = bool.get("should").toString();
    String mustNot = bool.get("must_not").toString();
    assertTrue(must.contains("from-must"));
    assertFalse(should.contains("from-must"));
    assertFalse(should.contains("from-must-not"));
    assertTrue(should.contains("from-should"));
    assertTrue(mustNot.contains("from-must-not"));
    assertTrue(bool.get("filter").toString().contains("\"removed\""));
    assertTrue(should.contains(harness.property("certification.status").fieldPath() + ".keyword"));
  }

  @Test
  public void testBoostListRendersOneWeightAndCannotBeSpoofed() {
    SearchTemplateProperties weighted = template("Viewer", "{ \"match_all\": {} }");
    SearchTemplateBoost viewer = new SearchTemplateBoost();
    viewer.setField("urn");
    viewer.setValues(SearchTemplateViewer.URN);
    viewer.setWeight(2.5);
    weighted.setBoosts(new SearchTemplateBoostArray(viewer));
    harness.store("urn:li:searchTemplate:viewer", weighted);
    String rendered = render("urn:li:searchTemplate:viewer", "orders", false);
    assertTrue(rendered.contains("\"weight\" : 2.5"));
    assertTrue(rendered.contains("\"terms\""));
    assertTrue(rendered.contains(SearchTemplateViewer.urn(harness.opContext)));

    SearchTemplateProperties empty = template("Domains", "{ \"match_all\": {} }");
    SearchTemplateBoost domains = new SearchTemplateBoost();
    domains.setField("domains");
    domains.setValues(SearchTemplateViewer.DOMAIN_LIST);
    domains.setWeight(1.5);
    empty.setBoosts(new SearchTemplateBoostArray(domains));
    empty.setParameters(
        new StringMap(Map.of(SearchTemplateViewer.DOMAIN_LIST, "[\"urn:li:domain:spoof\"]")));
    harness.store("urn:li:searchTemplate:domains", empty);
    String spoofed = render("urn:li:searchTemplate:domains", "orders", false);
    assertFalse(spoofed.contains("spoof"));
    assertFalse(spoofed.contains("1.5"));
    assertFalse(spoofed.contains("\"terms\""));
  }

  @Test
  public void testFieldAddAndReplacePlaceValueTerms() {
    SearchFieldBoost add = new SearchFieldBoost();
    add.setFieldName(harness.property("hidden.label").urn());
    add.setBoost(6.0f);
    SearchFieldOverrides added = new SearchFieldOverrides();
    added.setAdd(new SearchFieldBoostArray(add));
    SearchTemplateProperties addTemplate = template("Add", "{ \"match_all\": {} }");
    addTemplate.setFields(added);
    harness.store("urn:li:searchTemplate:add", addTemplate);
    String withAdd = render("urn:li:searchTemplate:add", "orders", true);
    assertTrue(withAdd.contains(harness.property("hidden.label").fieldPath() + ".keyword"));
    assertTrue(withAdd.contains("\"boost\" : 6.0"));

    SearchFieldBoost only = new SearchFieldBoost();
    only.setFieldName(harness.property("certification.status").urn());
    only.setBoost(3.0f);
    SearchFieldOverrides replace = new SearchFieldOverrides();
    replace.setReplace(new SearchFieldBoostArray(only));
    SearchTemplateProperties replaceTemplate = template("Replace", "{ \"match_all\": {} }");
    replaceTemplate.setFields(replace);
    harness.store("urn:li:searchTemplate:replace", replaceTemplate);
    String replaced = render("urn:li:searchTemplate:replace", "orders", true);
    assertTrue(
        replaced.contains(harness.property("certification.status").fieldPath() + ".keyword"));
    assertFalse(replaced.contains(harness.property("summary.notes").fieldPath()));
    assertFalse(replaced.contains(harness.property("owner.ref").fieldPath()));
    assertFalse(replaced.contains(harness.property("contract.id").fieldPath()));
    assertFalse(replaced.contains(harness.property("hidden.label").fieldPath()));
  }

  @Test
  public void testHighlightReplaceAndAggregationClamp() {
    SearchTemplateProperties properties = template("Facets", "{ \"match_all\": {} }");
    HighlightFieldOverrides highlights = new HighlightFieldOverrides();
    highlights.setReplace(new StringArray("glossaryTerm"));
    properties.setHighlightFields(highlights);
    properties.setAggregations(
        "{ \"templateFacet\": { \"terms\": { \"field\": \"platform.keyword\", \"size\": 500 } } }");
    harness.store("urn:li:searchTemplate:facets", properties);
    OperationContext bound =
        harness
            .opContext
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate("urn:li:searchTemplate:facets", 1L, "fulltext", null))
            .withSearchFlags(flags -> flags.setMaxAggValues(200));
    SearchRequest request = request(bound, harness.elastic());
    List<String> names =
        request.source().highlighter().fields().stream().map(field -> field.name()).toList();
    assertEquals(names, List.of("glossaryTerm"));
    TermsAggregationBuilder facet =
        request.source().aggregations().getAggregatorFactories().stream()
            .filter(aggregation -> "templateFacet".equals(aggregation.getName()))
            .map(TermsAggregationBuilder.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(facet.size(), 50);
  }

  @Test
  public void testScrollKeepsTemplateSortAndDropsRescoreAndKnn() {
    SearchTemplateProperties sorted = template("Sorted", "{ \"match_all\": {} }");
    sorted.setSort("[ { \"name\": { \"order\": \"asc\" } } ]");
    harness.store("urn:li:searchTemplate:sorted", sorted);
    SearchRequest request = scrollRequest("urn:li:searchTemplate:sorted");
    assertTrue(
        request.source().sorts().stream()
            .anyMatch(
                sort ->
                    sort instanceof FieldSortBuilder field && "name".equals(field.getFieldName())));
    assertTrue(request.source().rescores() == null || request.source().rescores().isEmpty());

    SearchTemplateProperties rescored = template("Rescored", "{ \"match_all\": {} }");
    rescored.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} }, \"query_weight\": 1.0, \"rescore_query_weight\": 1.0 } }");
    harness.store("urn:li:searchTemplate:rescored", rescored);
    SearchRequest withoutRescore = scrollRequest("urn:li:searchTemplate:rescored");
    assertTrue(
        withoutRescore.source().rescores() == null || withoutRescore.source().rescores().isEmpty());
    assertFalse(withoutRescore.source().toString().contains("embeddings"));

    SearchTemplateProperties knn = template("Knn", "{ \"match_all\": {} }");
    knn.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    harness.store(SEARCH_TEMPLATE_FULLTEXT_URN, knn);
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(harness.elastic(), null, harness.loader);
    OperationContext scrolled = coordinator.bind(harness.opContext, "customer orders", true);
    assertNotNull(scrolled.getSearchContext().getResolvedSearchTemplate());
    assertFalse(scrolled.getSearchContext().getResolvedSearchTemplate().isKnnApplied());
  }

  @Test
  public void testSupportedRankingAndKeywordRetrievalRender() {
    SearchTemplateProperties properties =
        template("Keyword", "{ \"term\": { \"name.keyword\": \"orders\" } }");
    SearchTemplateRanking ranking = new SearchTemplateRanking();
    ranking.setPreset("none");
    ranking.setWindowSizer("fixed");
    properties.setRanking(ranking);
    SearchTemplateRetrieval retrieval = new SearchTemplateRetrieval();
    retrieval.setMode("keyword");
    properties.setRetrieval(retrieval);
    SearchTemplateChecks.check(properties, 1024);
    harness.store(SEARCH_TEMPLATE_FULLTEXT_URN, properties);
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(harness.elastic(), null, harness.loader);
    OperationContext bound = coordinator.bind(harness.opContext, "customer orders");
    assertNotNull(bound.getSearchContext().getResolvedSearchTemplate());
    assertTrue(
        new SearchV3QueryBuilder(harness.loader, new SearchConfiguration(), null)
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "customer orders", true)
            .toString()
            .contains("orders"));
  }

  @Test
  public void testUnsetHoleIsAbsentFromFulltext() {
    harness.installShippedTemplates();
    String rendered = render(SEARCH_TEMPLATE_FULLTEXT_URN, "customer orders", true);
    assertFalse(rendered.contains("extraDisMaxQueries"), rendered);
  }

  @Test
  public void testFilledHoleJoinsFulltextDisMaxAtTheFragmentBoost() throws Exception {
    harness.installShippedTemplates();
    String certification = "urn:li:structuredProperty:certification.status";
    String path = harness.property("certification.status").fieldPath() + ".keyword";
    SearchTemplateProperties child = template("Business name", null);
    child.setParent(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN));
    child.setQueryFragments(
        new StringMap(
            Map.of(
                "extraDisMaxQueries",
                """
                {
                  "constant_score": {
                    "_name": "exact",
                    "boost": 1000,
                    "filter": {
                      "term": {
                        "{{%s}}.keyword": {
                          "value": "{{unquoted}}",
                          "case_insensitive": true
                        }
                      }
                    }
                  }
                }
                """
                    .formatted(certification))));
    SearchFieldBoost added = new SearchFieldBoost();
    added.setFieldName(certification);
    added.setBoost(4.0);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    fields.setAdd(new SearchFieldBoostArray(added));
    child.setFields(fields);
    harness.store("urn:li:searchTemplate:businessName", child);
    String rendered = render("urn:li:searchTemplate:businessName", "customer orders", true);
    assertFalse(rendered.contains(certification), rendered);
    assertEquals(constantScoreBoost(MAPPER.readTree(rendered), path), 1000.0, rendered);
    assertTrue(rendered.contains("\"value\" : \"customer orders\""), rendered);
  }

  @Test
  public void testHoleAcceptsSeveralBranchesAndDropsAnEmptyFragment() {
    SearchTemplateProperties parent = template("Parent", HOLE_QUERY);
    parent.setQueryFragments(
        new StringMap(
            Map.of("extraDisMaxQueries", "{ \"term\": { \"label\": \"from-parent\" } }")));
    harness.store("urn:li:searchTemplate:holeParent", parent);

    SearchTemplateProperties several = template("Several", null);
    several.setParent(UrnUtils.getUrn("urn:li:searchTemplate:holeParent"));
    several.setQueryFragments(
        new StringMap(
            Map.of(
                "extraDisMaxQueries",
                "{ \"term\": { \"a\": \"one\" } }, { \"term\": { \"b\": \"two\" } }")));
    harness.store("urn:li:searchTemplate:several", several);
    String branches = render("urn:li:searchTemplate:several", "orders", false);
    assertTrue(branches.contains("\"one\""), branches);
    assertTrue(branches.contains("\"two\""), branches);
    assertTrue(branches.contains("from-parent"), branches);

    SearchTemplateProperties dropped = template("Dropped", null);
    dropped.setParent(UrnUtils.getUrn("urn:li:searchTemplate:holeParent"));
    dropped.setQueryFragments(new StringMap(Map.of("extraDisMaxQueries", "")));
    harness.store("urn:li:searchTemplate:dropped", dropped);
    String empty = render("urn:li:searchTemplate:dropped", "orders", false);
    assertFalse(empty.contains("from-parent"), empty);
    assertTrue(empty.contains("match_all"), empty);
  }

  @Test
  public void testHoleJoinsFragmentsDownTheParentChain() throws Exception {
    SearchTemplateProperties root = template("Root", HOLE_QUERY);
    root.setQueryFragments(
        new StringMap(Map.of("extraDisMaxQueries", "{ \"term\": { \"label\": \"from-root\" } }")));
    harness.store("urn:li:searchTemplate:holeRoot", root);

    SearchTemplateProperties child = template("Child", null);
    child.setParent(UrnUtils.getUrn("urn:li:searchTemplate:holeRoot"));
    child.setQueryFragments(
        new StringMap(Map.of("extraDisMaxQueries", "{ \"term\": { \"label\": \"from-child\" } }")));
    harness.store("urn:li:searchTemplate:holeChild", child);

    SearchTemplateProperties grandchild = template("Grandchild", null);
    grandchild.setParent(UrnUtils.getUrn("urn:li:searchTemplate:holeChild"));
    grandchild.setQueryFragments(
        new StringMap(
            Map.of("extraDisMaxQueries", "{ \"term\": { \"label\": \"from-grandchild\" } }")));
    harness.store("urn:li:searchTemplate:holeGrandchild", grandchild);

    JsonNode queries =
        MAPPER
            .readTree(render("urn:li:searchTemplate:holeGrandchild", "orders", false))
            .get("dis_max")
            .get("queries");
    assertEquals(queries.size(), 4);
    assertEquals(termValue(queries.get(1), "label"), "from-root");
    assertEquals(termValue(queries.get(2), "label"), "from-child");
    assertEquals(termValue(queries.get(3), "label"), "from-grandchild");
  }

  @Test
  public void testFragmentsDoNotSeeEachOther() throws Exception {
    SearchTemplateProperties parent =
        template(
            "Two holes",
            """
            {
              "bool": {
                "should": [
                  { "match_all": {} }
                  {{#first}}
                  ,{{&first}}
                  {{/first}}
                  {{#second}}
                  ,{{&second}}
                  {{/second}}
                ]
              }
            }
            """);
    parent.setQueryFragments(
        new StringMap(
            Map.of(
                "first",
                "{ \"term\": { \"a\": \"{{unquoted}}\" } }",
                "second",
                "{ \"term\": { \"b\": \"{{first}}\" } }")));
    harness.store("urn:li:searchTemplate:twoHoles", parent);
    String rendered = render("urn:li:searchTemplate:twoHoles", "orders", false);
    JsonNode root = MAPPER.readTree(rendered);
    assertEquals(termValue(root, "a"), "orders", rendered);
    assertEquals(termValue(root, "b"), "", rendered);
  }

  @Test
  public void testRenderFailsWhenAFilledHoleIsMissing() {
    SearchTemplateProperties properties = template("Missing", "{ \"match_all\": {} }");
    properties.setQueryFragments(
        new StringMap(Map.of("extraDisMaxQueries", "{ \"match_all\": {} }")));
    harness.store("urn:li:searchTemplate:missingHole", properties);
    assertThrows(
        SearchTemplateException.class,
        () -> render("urn:li:searchTemplate:missingHole", "orders", false));
  }

  @Test
  public void testBodyReferenceRendersAnalyzedPath() {
    String certification = "urn:li:structuredProperty:certification.status";
    String path = harness.property("certification.status").fieldPath();
    SearchTemplateProperties properties =
        template(
            "Analyzed path",
            "{ \"match\": { \"{{" + certification + "}}.text\": { \"query\": \"{{query}}\" } } }");
    harness.store("urn:li:searchTemplate:analyzedPath", properties);
    String rendered = render("urn:li:searchTemplate:analyzedPath", "gold", true);
    assertTrue(rendered.contains(path + ".text"), rendered);
    assertFalse(rendered.contains(certification), rendered);
  }

  @Test
  public void testRemovingFieldStripsBodyClauses() {
    String certification = "urn:li:structuredProperty:certification.status";
    String path = harness.property("certification.status").fieldPath();
    SearchTemplateProperties properties =
        template(
            "Strip removed",
            """
            {
              "bool": {
                "should": [
                  { "term": { "{{%s}}.keyword": { "value": "{{query}}" } } },
                  {
                    "simple_query_string": {
                      "query": "{{query}}",
                      "fields": ["{{%s}}.text^3", "name.text^10"]
                    }
                  }
                ]
              }
            }
            """
                .formatted(certification, certification));
    SearchFieldOverrides fields = new SearchFieldOverrides();
    fields.setRemove(new StringArray(certification));
    properties.setFields(fields);
    harness.store("urn:li:searchTemplate:stripRemoved", properties);
    String rendered = render("urn:li:searchTemplate:stripRemoved", "gold", true);
    assertFalse(rendered.contains(path), rendered);
    assertTrue(rendered.contains("name.text^10") || rendered.contains("name.text"), rendered);
  }

  @Test
  public void testOverrideChangesListAndValueTermNotNameAndValueWeight() {
    harness.installShippedTemplates();
    String certification = "urn:li:structuredProperty:certification.status";
    SearchTemplateProperties weighted = template("Override weight", null);
    weighted.setParent(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN));
    StructuredPropertyQuery section = new StructuredPropertyQuery();
    section.setNameAndValueWeight(8.0f);
    weighted.setStructuredPropertyQuery(section);
    SearchFieldBoost boost = new SearchFieldBoost();
    boost.setFieldName(certification);
    boost.setBoost(9.0);
    SearchFieldOverrides override = new SearchFieldOverrides();
    override.setOverride(new SearchFieldBoostArray(boost));
    weighted.setFields(override);
    harness.store("urn:li:searchTemplate:overrideWeight", weighted);
    String mixed =
        render("urn:li:searchTemplate:overrideWeight", "certification status gold", true);
    assertFalse(mixed.contains(harness.property("certification.status").fieldPath() + ".text^9"));
    assertTrue(mixed.contains("\"boost\" : 9.0"));
    assertTrue(mixed.contains("\"boost\" : 8.0"));
    assertTrue(mixed.contains("\"_name\" : \"nameAndValue\""));
    assertFalse(mixed.contains("\"boost\" : 900.0"));

    SearchTemplateProperties number = template("Number stays typed", "{ \"match_all\": {} }");
    SearchFieldBoost numberBoost = new SearchFieldBoost();
    numberBoost.setFieldName("urn:li:structuredProperty:priority.rank");
    numberBoost.setBoost(1.0);
    SearchFieldOverrides numberFields = new SearchFieldOverrides();
    numberFields.setOverride(new SearchFieldBoostArray(numberBoost));
    number.setFields(numberFields);
    SearchTemplateChecks.check(number, 1024, harness.opContext, harness.retriever, null);
    harness.store("urn:li:searchTemplate:numberExact", number);
    assertFalse(
        render("urn:li:searchTemplate:numberExact", "gold", true)
            .contains(harness.property("priority.rank").fieldPath()));
  }

  @Test
  public void testStructuredPropertyMatchReportsUrnValueAndMode() {
    String urn = harness.property("certification.status").urn();
    String path = harness.property("certification.status").fieldPath();
    String summary = harness.property("summary.notes").fieldPath();
    BoolQueryBuilder unique = QueryBuilders.boolQuery();
    unique.should(
        QueryBuilders.termQuery(path + ".keyword", "Gold")
            .queryName(SearchTemplateMatchReport.EXACT));
    unique.should(
        QueryBuilders.termQuery(path + ".keyword", "gold")
            .queryName(SearchTemplateMatchReport.NAME_AND_VALUE));
    Map<String, RenderedSearchTemplate.NamedClause> namedClauses =
        SearchTemplateMatchReport.namedClauses(unique);

    Map<String, Set<String>> matched = new LinkedHashMap<>();
    matched.put(path, Set.of("Gold"));
    matched.put(SearchTemplateMatchReport.EXACT, Set.of(""));
    matched.put(SearchTemplateMatchReport.NAME_AND_VALUE, Set.of("gold"));
    matched.put("name", Set.of("orders"));
    Map<String, Object> properties = new LinkedHashMap<>();
    properties.put(path.substring(path.indexOf('.') + 1), "Gold");
    List<MatchedField> fields =
        SearchTemplateMatchReport.fields(
            matched, Map.of("structuredProperties", properties), "Gold", namedClauses);
    assertEquals(fields.size(), 3);
    MatchedField exact =
        fields.stream()
            .filter(field -> SearchTemplateMatchReport.EXACT.equals(field.getMode()))
            .findFirst()
            .orElseThrow();
    assertEquals(exact.getName(), urn);
    assertEquals(exact.getValue(), "Gold");
    MatchedField named =
        fields.stream()
            .filter(field -> SearchTemplateMatchReport.NAME_AND_VALUE.equals(field.getMode()))
            .findFirst()
            .orElseThrow();
    assertEquals(named.getName(), urn);
    assertEquals(named.getValue(), "gold");
    MatchedField shared =
        fields.stream().filter(field -> "name".equals(field.getName())).findFirst().orElseThrow();
    assertFalse(shared.hasMode());

    BoolQueryBuilder reused = QueryBuilders.boolQuery();
    reused.should(
        QueryBuilders.termQuery(path + ".keyword", "Gold")
            .queryName(SearchTemplateMatchReport.EXACT));
    reused.should(
        QueryBuilders.termQuery(summary + ".keyword", "Gold")
            .queryName(SearchTemplateMatchReport.EXACT));
    namedClauses = SearchTemplateMatchReport.namedClauses(reused);
    Map<String, Set<String>> again = new LinkedHashMap<>();
    again.put(SearchTemplateMatchReport.EXACT, Set.of("Gold"));
    again.put(path, Set.of("Gold"));
    List<MatchedField> unset =
        SearchTemplateMatchReport.fields(again, Map.of(), "Gold", namedClauses);
    assertTrue(unset.stream().noneMatch(MatchedField::hasMode));
    assertTrue(unset.stream().anyMatch(field -> path.equals(field.getName())));
  }

  @Test
  public void testLadderNextRendersOnlyItsOwnSections() {
    SearchTemplateProperties previous = template("Previous", "{ \"match_all\": {} }");
    previous.setClauses("{ \"must\": [ { \"term\": { \"platform\": \"parent-rung\" } } ] }");
    previous.setFunctionScore(
        "{ \"functions\": [], \"score_mode\": \"multiply\", \"boost_mode\": \"multiply\" }");
    StructuredPropertyQuery weight = new StructuredPropertyQuery();
    weight.setNameAndValueWeight(8.0f);
    previous.setStructuredPropertyQuery(weight);
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    SearchTemplateLadderStop stop = new SearchTemplateLadderStop();
    stop.setMinHits(1);
    ladder.setStop(new SearchTemplateLadderStopArray(stop));
    ladder.setNext(UrnUtils.getUrn("urn:li:searchTemplate:served"));
    previous.setLadder(ladder);
    harness.store("urn:li:searchTemplate:previous", previous);

    SearchTemplateProperties served =
        template("Served", "{ \"term\": { \"name.keyword\": \"served-rung\" } }");
    harness.store("urn:li:searchTemplate:served", served);

    OperationContext bound =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate("urn:li:searchTemplate:previous", 1L, "fulltext", null));
    String own =
        new SearchV3QueryBuilder(harness.loader, new SearchConfiguration(), null)
            .render(bound, List.of(dataset), "certification status gold", true)
            .toString();
    assertTrue(own.contains("parent-rung"));
    assertTrue(own.contains("\"tie_breaker\" : 0.0"));

    String next =
        new SearchV3QueryBuilder(harness.loader, new SearchConfiguration(), null)
            .render(
                bound,
                List.of(dataset),
                "certification status gold",
                true,
                "urn:li:searchTemplate:served",
                false)
            .query()
            .toString();
    assertTrue(next.contains("served-rung"));
    assertFalse(next.contains("parent-rung"));
    assertFalse(next.contains("function_score"));
    assertFalse(next.contains("\"tie_breaker\" : 0.0"));
  }

  private String render(String urn, String query, boolean datasetSpec) {
    OperationContext bound =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(urn, 1L, "fulltext", null));
    QueryBuilder rendered =
        new SearchV3QueryBuilder(harness.loader, new SearchConfiguration(), null)
            .render(
                bound,
                List.of(datasetSpec ? dataset : TestEntitySpecBuilder.getSpec()),
                query,
                true);
    return rendered.toString();
  }

  private SearchRequest scrollRequest(String urn) {
    OperationContext bound =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(urn, 1L, "fulltext", null));
    return SearchRequestHandler.getBuilder(
            bound,
            TestEntitySpecBuilder.getSpec(),
            harness.elastic(),
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG)
        .getSearchRequest(bound, "orders", null, null, null, "pit", "1m", 10, List.of());
  }

  private SearchRequest request(OperationContext context, ElasticSearchConfiguration elastic) {
    return SearchRequestHandler.getBuilder(
            context,
            TestEntitySpecBuilder.getSpec(),
            elastic,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG)
        .getSearchRequest(context, "orders", null, null, 0, 10, List.of());
  }

  private static Double constantScoreBoost(JsonNode node, String field) {
    if (node == null) {
      return null;
    }
    if (node.isObject()) {
      JsonNode score = node.get("constant_score");
      if (score != null && score.toString().contains(field) && score.has("boost")) {
        return score.get("boost").asDouble();
      }
      Double found = null;
      var fields = node.fields();
      while (fields.hasNext()) {
        found = constantScoreBoost(fields.next().getValue(), field);
        if (found != null) {
          return found;
        }
      }
      return null;
    }
    if (node.isArray()) {
      for (JsonNode child : node) {
        Double found = constantScoreBoost(child, field);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static String termValue(JsonNode node, String field) {
    if (node == null) {
      return null;
    }
    if (node.isObject()) {
      JsonNode term = node.get("term");
      if (term != null && term.has(field)) {
        JsonNode value = term.get(field);
        if (value.isObject() && value.has("value")) {
          return value.get("value").asText();
        }
        return value.asText();
      }
      String found = null;
      var fields = node.fields();
      while (fields.hasNext()) {
        found = termValue(fields.next().getValue(), field);
        if (found != null) {
          return found;
        }
      }
      return null;
    }
    if (node.isArray()) {
      for (JsonNode child : node) {
        String found = termValue(child, field);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static SearchTemplateProperties template(String name, String query) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(name);
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    if (query != null) {
      properties.setQuery(query);
    }
    return properties;
  }
}
