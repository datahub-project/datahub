package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.TestEntitySpecBuilder;
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
import com.linkedin.search.template.SearchTemplateRetrievalBridge;
import java.util.List;
import java.util.Map;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SearchTemplateValidationTest {
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

  @BeforeMethod
  public void setup() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
  }

  @AfterMethod
  public void clear() {
    SearchTemplates.install(null);
  }

  @DataProvider
  public static Object[][] invalidTemplates() {
    return new Object[][] {
      {"topLevelFunctionScore"},
      {"clausesNotJson"},
      {"aggregationsNotJson"},
      {"sortNotJson"},
      {"replaceWithAdd"},
      {"replaceWithRemove"},
      {"replaceWithOverride"},
      {"overrideMissing"},
      {"boostValues"},
      {"functionScoreViewerUrn"},
      {"functionScoreViewerGroups"},
      {"functionScoreViewerDomains"},
      {"functionScoreDomainsJson"},
      {"windowSizer"},
      {"vectorMode"},
      {"retrievalBridge"},
      {"mergeJoin"},
      {"parentCycle"},
      {"suffixTextOnDate"},
      {"suffixTextOnNumber"},
      {"suffixTextOnUrn"},
      {"suffixKeywordOnDate"},
      {"suffixKeywordOnNumber"},
      {"suffixKeywordOnUrn"},
      {"fieldsSubfieldText"}
    };
  }

  @Test(dataProvider = "invalidTemplates")
  public void testInvalidTemplate(String name) {
    assertThrows(SearchTemplateException.class, () -> reject(name));
  }

  private void reject(String name) {
    switch (name) {
      case "topLevelFunctionScore" ->
          SearchTemplateChecks.check(
              template(
                  "{ \"function_score\": { \"query\": { \"match_all\": {} } } }", null, null, null),
              1024);
      case "clausesNotJson" ->
          SearchTemplateChecks.check(
              template("{ \"match_all\": {} }", "not-json", null, null), 1024);
      case "aggregationsNotJson" ->
          SearchTemplateChecks.check(
              template("{ \"match_all\": {} }", null, "not-json", null), 1024);
      case "sortNotJson" ->
          SearchTemplateChecks.check(
              template("{ \"match_all\": {} }", null, null, "not-json"), 1024);
      case "replaceWithAdd" -> SearchTemplateChecks.check(fieldReplace(true, false, false), 1024);
      case "replaceWithRemove" ->
          SearchTemplateChecks.check(fieldReplace(false, true, false), 1024);
      case "replaceWithOverride" ->
          SearchTemplateChecks.check(fieldReplace(false, false, true), 1024);
      case "overrideMissing" ->
          SearchTemplateChecks.check(
              overrideMissing(), 1024, null, null, List.of(TestEntitySpecBuilder.getSpec()));
      case "boostValues" -> SearchTemplateChecks.check(boostValues(), 1024);
      case "functionScoreViewerUrn" -> SearchTemplateChecks.check(functionScore("viewerUrn"), 1024);
      case "functionScoreViewerGroups" ->
          SearchTemplateChecks.check(functionScore("viewerGroupsJson"), 1024);
      case "functionScoreViewerDomains" ->
          SearchTemplateChecks.check(functionScore("viewerDomainsJson"), 1024);
      case "functionScoreDomainsJson" ->
          SearchTemplateChecks.check(functionScore("domainsJson"), 1024);
      case "windowSizer" -> bind(ranking("tokenCurve"));
      case "vectorMode" -> bind(retrieval("vector", false));
      case "retrievalBridge" -> bind(retrieval("keyword", true));
      case "mergeJoin" -> bind(merge());
      case "parentCycle" -> parentCycle();
      case "suffixTextOnDate" -> suffix("urn:li:structuredProperty:due.on", "text");
      case "suffixTextOnNumber" -> suffix("urn:li:structuredProperty:priority.rank", "text");
      case "suffixTextOnUrn" -> suffix("urn:li:structuredProperty:owner.ref", "text");
      case "suffixKeywordOnDate" -> suffix("urn:li:structuredProperty:due.on", "keyword");
      case "suffixKeywordOnNumber" -> suffix("urn:li:structuredProperty:priority.rank", "keyword");
      case "suffixKeywordOnUrn" -> suffix("urn:li:structuredProperty:owner.ref", "keyword");
      case "fieldsSubfieldText" -> fieldsSubfieldText();
      default -> throw new IllegalArgumentException(name);
    }
  }

  private void bind(SearchTemplateProperties properties) {
    harness.store(SEARCH_TEMPLATE_FULLTEXT_URN, properties);
    new SearchTemplateCoordinator(harness.elastic(), null, harness.loader)
        .bind(harness.opContext, "customer orders");
  }

  private void parentCycle() {
    SearchTemplateProperties left = template("{ \"match_all\": {} }", null, null, null);
    left.setParent(UrnUtils.getUrn("urn:li:searchTemplate:right"));
    SearchTemplateProperties right = template("{ \"match_all\": {} }", null, null, null);
    right.setParent(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN));
    harness.store(SEARCH_TEMPLATE_FULLTEXT_URN, left);
    harness.store("urn:li:searchTemplate:right", right);
    harness.loader.load(harness.opContext, SEARCH_TEMPLATE_FULLTEXT_URN);
  }

  private void checkMerged(SearchTemplateProperties child) {
    MergedSearchTemplate merged =
        harness.loader.preview(
            harness.opContext, harness.retriever, "urn:li:searchTemplate:child", child);
    SearchTemplateChecks.checkMerged(merged, 1024, harness.opContext, harness.retriever, null);
  }

  private static SearchTemplateProperties childOf(String parent) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName("Child");
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    properties.setParent(UrnUtils.getUrn(parent));
    return properties;
  }

  private static SearchTemplateProperties template(
      String query, String clauses, String aggregations, String sort) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName("Invalid");
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    properties.setQuery(query);
    if (clauses != null) {
      properties.setClauses(clauses);
    }
    if (aggregations != null) {
      properties.setAggregations(aggregations);
    }
    if (sort != null) {
      properties.setSort(sort);
    }
    return properties;
  }

  @Test
  public void testChildFragmentRequiresAHoleInTheInheritedQuery() {
    harness.store(
        SEARCH_TEMPLATE_FULLTEXT_URN, template("{ \"match_all\": {} }", null, null, null));
    SearchTemplateProperties child = childOf(SEARCH_TEMPLATE_FULLTEXT_URN);
    child.setQueryFragments(new StringMap(Map.of("extraDisMaxQueries", "{ \"match_all\": {} }")));
    assertThrows(SearchTemplateException.class, () -> checkMerged(child));
  }

  @Test
  public void testFragmentMustLeaveAValidQuery() {
    harness.store(SEARCH_TEMPLATE_FULLTEXT_URN, template(HOLE_QUERY, null, null, null));
    SearchTemplateProperties valid = childOf(SEARCH_TEMPLATE_FULLTEXT_URN);
    valid.setQueryFragments(
        new StringMap(
            Map.of(
                "extraDisMaxQueries", "{ \"match_all\": {} }, { \"term\": { \"b\": \"two\" } }")));
    checkMerged(valid);

    SearchTemplateProperties broken = childOf(SEARCH_TEMPLATE_FULLTEXT_URN);
    broken.setQueryFragments(new StringMap(Map.of("extraDisMaxQueries", "not-json")));
    assertThrows(SearchTemplateException.class, () -> checkMerged(broken));
  }

  @Test
  public void testBoostSubfieldTextOnString() {
    harness.installCatalog();
    SearchTemplateProperties properties = template("{ \"match_all\": {} }", null, null, null);
    SearchTemplateBoost boost = new SearchTemplateBoost();
    boost.setField("urn:li:structuredProperty:certification.status");
    boost.setSubfield("text");
    boost.setValues("domainsJson");
    boost.setWeight(1.5);
    properties.setBoosts(new SearchTemplateBoostArray(boost));
    SearchTemplateChecks.check(properties, 1024, harness.opContext, harness.retriever, null);
  }

  private void suffix(String urn, String form) {
    harness.installCatalog();
    SearchTemplateChecks.check(
        template(
            "{ \"term\": { \"{{" + urn + "}}." + form + "\": { \"value\": \"x\" } } }",
            null,
            null,
            null),
        1024,
        harness.opContext,
        harness.retriever,
        null);
  }

  private void fieldsSubfieldText() {
    harness.installCatalog();
    SearchTemplateProperties properties = template("{ \"match_all\": {} }", null, null, null);
    SearchFieldBoost boost = new SearchFieldBoost();
    boost.setFieldName("urn:li:structuredProperty:certification.status");
    boost.setBoost(1.0);
    boost.setSubfield("text");
    SearchFieldOverrides fields = new SearchFieldOverrides();
    fields.setAdd(new SearchFieldBoostArray(boost));
    properties.setFields(fields);
    SearchTemplateChecks.check(properties, 1024, harness.opContext, harness.retriever, null);
  }

  private static SearchTemplateProperties fieldReplace(
      boolean add, boolean remove, boolean override) {
    SearchTemplateProperties properties = template("{ \"match_all\": {} }", null, null, null);
    SearchFieldBoost replaced = new SearchFieldBoost();
    replaced.setFieldName("name");
    replaced.setBoost(1.0f);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    fields.setReplace(new SearchFieldBoostArray(replaced));
    if (add) {
      SearchFieldBoost boost = new SearchFieldBoost();
      boost.setFieldName("description");
      boost.setBoost(2.0f);
      fields.setAdd(new SearchFieldBoostArray(boost));
    }
    if (remove) {
      fields.setRemove(new StringArray("description"));
    }
    if (override) {
      SearchFieldBoost boost = new SearchFieldBoost();
      boost.setFieldName("name");
      boost.setBoost(2.0f);
      fields.setOverride(new SearchFieldBoostArray(boost));
    }
    properties.setFields(fields);
    return properties;
  }

  private static SearchTemplateProperties overrideMissing() {
    SearchTemplateProperties properties = template("{ \"match_all\": {} }", null, null, null);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    SearchFieldBoost boost = new SearchFieldBoost();
    boost.setFieldName("notInTheQueriedSet");
    boost.setBoost(2.0f);
    fields.setOverride(new SearchFieldBoostArray(boost));
    properties.setFields(fields);
    return properties;
  }

  private static SearchTemplateProperties boostValues() {
    SearchTemplateProperties properties = template("{ \"match_all\": {} }", null, null, null);
    SearchTemplateBoost boost = new SearchTemplateBoost();
    boost.setField("domains");
    boost.setValues("notInjectable");
    boost.setWeight(1.5);
    properties.setBoosts(new SearchTemplateBoostArray(boost));
    return properties;
  }

  private static SearchTemplateProperties functionScore(String injectable) {
    return template("{ \"match_all\": {} }", null, null, null)
        .setFunctionScore("{ \"functions\": [], \"" + injectable + "\": [] }");
  }

  private static SearchTemplateProperties ranking(String windowSizer) {
    SearchTemplateProperties properties = template("{ \"match_all\": {} }", null, null, null);
    SearchTemplateRanking ranking = new SearchTemplateRanking();
    ranking.setPreset("none");
    ranking.setWindowSizer(windowSizer);
    properties.setRanking(ranking);
    return properties;
  }

  private static SearchTemplateProperties retrieval(String mode, boolean bridge) {
    SearchTemplateProperties properties = template("{ \"match_all\": {} }", null, null, null);
    SearchTemplateRetrieval retrieval = new SearchTemplateRetrieval();
    retrieval.setMode(mode);
    if (bridge) {
      SearchTemplateRetrievalBridge leg = new SearchTemplateRetrievalBridge();
      leg.setPivotField("urn");
      retrieval.setBridge(leg);
    }
    properties.setRetrieval(retrieval);
    return properties;
  }

  private static SearchTemplateProperties merge() {
    SearchTemplateProperties properties = template("{ \"match_all\": {} }", null, null, null);
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setJoin("merge");
    ladder.setParallel(new UrnArray(UrnUtils.getUrn("urn:li:searchTemplate:member")));
    SearchTemplateLadderStop stop = new SearchTemplateLadderStop();
    stop.setMinHits(1);
    ladder.setStop(new SearchTemplateLadderStopArray(stop));
    properties.setLadder(ladder);
    return properties;
  }
}
