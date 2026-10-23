package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.search.template.HighlightFieldOverrides;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateBoost;
import com.linkedin.search.template.SearchTemplateBoostArray;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class SearchTemplateMergeTest extends SearchTemplateFixture {
  @Test
  public void testRenderAndParentMerge() {
    SearchTemplateProperties fulltext = body("Fulltext", FULLTEXT_QUERY, null);
    fulltext.setFunctionScore(
        """
        { "functions": [], "score_mode": "multiply", "boost_mode": "multiply" }
        """);
    fulltext.setParameters(new StringMap(Map.of("deprecatedBoost", "0.25")));
    store(SEARCH_TEMPLATE_FULLTEXT_URN, fulltext, 50L);
    String child = "urn:li:searchTemplate:mergeChild";
    SearchTemplateProperties lineage = parentOf("Child", SEARCH_TEMPLATE_FULLTEXT_URN);
    store(child, lineage, 20L);

    SearchTemplateRenderer renderer =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null);
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(child, 50L, "fulltext", null));
    String rendered =
        renderer.render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true).toString();
    assertTrue(rendered.contains("simple_query_string"));
    assertTrue(rendered.contains("orders"));
    assertTrue(rendered.contains("function_score"));
    assertTrue(rendered.contains("multiply"));
    assertTrue(bound.getSearchContext().getResolvedSearchTemplate().getHighlightFields().isEmpty());

    MergedSearchTemplate merged = loader.load(opContext, child);
    assertTrue(merged.getChain().contains(SEARCH_TEMPLATE_FULLTEXT_URN));
    assertEquals(merged.getLastModified(), 50L);
    fulltext.setQuery("{ \"term\": { \"name\": \"{{query}}\" } }");
    store(SEARCH_TEMPLATE_FULLTEXT_URN, fulltext);
    assertTrue(loader.load(opContext, child).getQuery().contains("bool"));
    expire();
    assertTrue(loader.load(opContext, child).getQuery().contains("term"));
  }

  @Test
  public void testFieldLayersIgnoreSearchTier() {
    List<SearchFieldConfig> pdl =
        SearchTemplateFields.pdlQueryFields(List.of(TestEntitySpecBuilder.getSpec()));
    assertFalse(pdl.isEmpty());
    assertTrue(pdl.stream().noneMatch(field -> field.fieldName().contains("_search.tier")));
    SearchFieldOverrides remove = new SearchFieldOverrides();
    remove.setRemove(new StringArray(pdl.get(0).fieldName()));
    List<SearchFieldConfig> without = SearchTemplateFields.applyTemplate(pdl, remove);
    assertEquals(without.size(), pdl.size() - 1);
    SearchFieldOverrides replace = new SearchFieldOverrides();
    SearchFieldBoost boost = new SearchFieldBoost();
    boost.setFieldName("name");
    boost.setBoost(10.0);
    replace.setReplace(new SearchFieldBoostArray(boost));
    List<SearchFieldConfig> replaced = SearchTemplateFields.applyTemplate(pdl, replace);
    assertEquals(replaced.size(), 1);
    assertEquals(replaced.get(0).fieldName(), "name");
    String existing = pdl.get(0).fieldName();
    SearchFieldOverrides override = new SearchFieldOverrides();
    SearchFieldBoost weight = new SearchFieldBoost();
    weight.setFieldName(existing);
    weight.setBoost(10.0);
    override.setOverride(new SearchFieldBoostArray(weight));
    List<SearchFieldConfig> overridden = SearchTemplateFields.applyTemplate(pdl, override);
    assertEquals(overridden.size(), pdl.size());
    assertEquals(
        overridden.stream()
            .filter(field -> field.fieldName().equals(existing))
            .findFirst()
            .get()
            .boost(),
        10.0f);
    SearchFieldOverrides missing = new SearchFieldOverrides();
    SearchFieldBoost unknown = new SearchFieldBoost();
    unknown.setFieldName("notAQueriedField");
    unknown.setBoost(3.0);
    missing.setOverride(new SearchFieldBoostArray(unknown));
    assertThrows(
        SearchTemplateException.class, () -> SearchTemplateFields.applyTemplate(pdl, missing));
    assertEquals(
        SearchTemplateFields.applyTemplate(pdl, new SearchFieldOverrides()).size(), pdl.size());
    assertEquals(
        SearchTemplateFields.applyHighlight(
                SearchTemplateFields.pdlHighlightFields(List.of(TestEntitySpecBuilder.getSpec())),
                new HighlightFieldOverrides())
            .size(),
        pdl.size());
  }

  @Test
  public void testChecks() {
    SearchTemplateProperties ok = body("Ok", "{ \"match_all\": {} }", LIST_SCORE);
    SearchTemplateChecks.check(ok, 1024);

    SearchTemplateProperties removed = body("Removed", "{ \"term\": { \"removed\": true } }", null);
    SearchTemplateChecks.check(removed, 1024);
    SearchTemplateProperties entityType =
        body("Type", "{ \"term\": { \"_entityType\": \"dataset\" } }", null);
    SearchTemplateChecks.check(entityType, 1024);

    SearchTemplateProperties script =
        body("Script", "{ \"script\": { \"script\": { \"source\": \"1\" } } }", null);
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(script, 1024));

    SearchTemplateProperties lookup =
        body(
            "Lookup",
            "{ \"terms\": { \"user\": { \"index\": \"users\", \"id\": \"1\", \"path\": \"followers\" } } }",
            null);
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(lookup, 1024));

    SearchTemplateProperties minScore = body("Min", "{ \"match_all\": {} }", null);
    minScore.setFunctionScore("{ \"functions\": [], \"min_score\": 1 }");
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(minScore, 1024));

    SearchTemplateProperties rescoreReplace = body("Rescore", "{ \"match_all\": {} }", LIST_SCORE);
    rescoreReplace.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} } } }");
    assertThrows(
        SearchTemplateException.class, () -> SearchTemplateChecks.check(rescoreReplace, 1024));

    SearchTemplateProperties partial =
        body("Partial", "{ \"match_all\": {} } {{> /etc/passwd}}", null);
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(partial, 1024));

    SearchTemplateProperties unknown =
        body("Unknown", "{ \"no_such_query\": { \"foo\": \"bar\" } }", null);
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(unknown, 1024));

    SearchTemplateProperties knn = body("Knn", "{ \"match_all\": {} }", null);
    knn.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    SearchTemplateChecks.check(knn, 1024);

    SearchTemplateProperties knnScript = body("KnnScript", "{ \"match_all\": {} }", null);
    knnScript.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10, \"script\": {} }");
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(knnScript, 1024));

    SearchTemplateProperties knnSort = body("KnnSort", "{ \"match_all\": {} }", null);
    knnSort.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    knnSort.setSort("[ { \"_score\": { \"order\": \"desc\" } } ]");
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(knnSort, 1024));

    SearchTemplateProperties sortedRescore = body("Sorted", "{ \"match_all\": {} }", null);
    sortedRescore.setSort("[ { \"_score\": { \"order\": \"desc\" } } ]");
    sortedRescore.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} } } }");
    assertThrows(
        SearchTemplateException.class, () -> SearchTemplateChecks.check(sortedRescore, 1024));

    StringBuilder clauses = new StringBuilder();
    for (int i = 0; i < 5; i++) {
      if (i > 0) {
        clauses.append(", ");
      }
      clauses.append("{ \"match_all\": {} }");
    }
    SearchTemplateProperties probe =
        body("Probe", "{ \"bool\": { \"should\": [ " + clauses + " ] } }", null);
    SearchTemplateException overflow =
        expectThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(probe, 4));
    assertTrue(overflow.getMessage().contains("leaf clauses"));

    SearchTemplateProperties postgres = body("Pg", "to_tsquery('simple', '{{query}}')", null);
    postgres.setDialect(SearchTemplateDialect.POSTGRES);
    postgres.setFunctionScore("not json {{query}}");
    SearchTemplateChecks.check(postgres, 1024);
  }

  @Test
  public void testUnescapedUserTextIsRejected() {
    assertThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateChecks.check(
                body("Triple", "{ \"match\": { \"name\": { \"query\": \"{{{query}}}\" } } }", null),
                1024));
    assertThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateChecks.check(
                body("Delimiter", "{ \"match_all\": {} } {{=<% %>=}}", null), 1024));
    for (String name : List.of("query", "raw", "escaped", "unquoted", "notAContextName")) {
      assertThrows(
          SearchTemplateException.class,
          () ->
              SearchTemplateChecks.check(
                  body(
                      "Unescaped",
                      "{ \"match\": { \"name\": { \"query\": \"{{&" + name + "}}\" } } }",
                      null),
                  1024));
    }
    SearchTemplateChecks.check(
        body(
            "Fields",
            "{ \"simple_query_string\": { \"query\": \"{{query}}\", \"fields\": {{&defaultQueryFieldsJson}} } }",
            null),
        1024);
    SearchTemplateChecks.check(
        body(
            "Structured",
            "{ \"bool\": { \"should\": [ {{&structuredQuery}}, { \"match_all\": {} } ], \"minimum_should_match\": 1 } }",
            null),
        1024);
    SearchTemplateChecks.check(body("Hole", HOLE_QUERY, null), 1024);
  }

  @Test
  public void testSynonymSampleIsCountedAtTheCap() {
    assertEquals(
        ((List<?>) SearchTemplateMustache.validationContext("").get("synonyms")).size(),
        SearchTemplateNormalizationPolicy.MAX_SYNONYM_CAP);
    String heavy = synonymBlock(200);
    Map<String, Object> once = new LinkedHashMap<>(SearchTemplateMustache.validationContext(heavy));
    once.put("synonyms", List.of("production"));
    int oneCopy = SearchTemplateJson.countLeaves(SearchTemplateMustache.render(heavy, once));
    assertTrue(oneCopy <= 1024, String.valueOf(oneCopy));
    assertTrue(oneCopy * SearchTemplateNormalizationPolicy.MAX_SYNONYM_CAP > 1024);
    assertThrows(
        SearchTemplateException.class,
        () -> SearchTemplateChecks.check(body("Heavy", heavy, null), 1024));
    SearchTemplateChecks.check(body("Within", synonymBlock(1), null), 1024);
  }

  private static String synonymBlock(int copies) {
    StringBuilder clauses = new StringBuilder();
    for (int i = 0; i < copies; i++) {
      if (i > 0) {
        clauses.append(',');
      }
      clauses.append("{ \"match_all\": {} }");
    }
    return """
        {
          "bool": {
            "should": [
              {{#synonyms}}
              %s,
              {{/synonyms}}
              { "match_all": {} }
            ]
          }
        }
        """
        .formatted(clauses);
  }

  @Test
  public void testRenderUsesEntityTypesFactorsAndStripsColons() {
    SearchConfiguration search = new SearchConfiguration();
    ExactMatchConfiguration exact = new ExactMatchConfiguration();
    exact.setPrefixFactor(1.1f);
    search.setExactMatch(exact);
    String entityName = TestEntitySpecBuilder.getSpec().getName();
    store(
        SEARCH_TEMPLATE_FULLTEXT_URN,
        body(
            "Fulltext",
            "{ \"term\": { \""
                + entityName
                + "\": { \"value\": \"{{query}}\", \"boost\": {{prefixFactor}} } } }",
            null));
    store(
        "urn:li:searchTemplate:other",
        body("Other", "{ \"term\": { \"name\": \"{{query}}-other\" } }", null));
    SearchTemplateRenderer renderer = new SearchTemplateRenderer(loader, search, null);
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    String rendered =
        renderer
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), ":orders", true)
            .toString();
    assertTrue(
        rendered.contains("\"boost\" : 1.1") || rendered.contains("\"boost\":1.1"), rendered);
    assertFalse(rendered.contains("1.100000023841858"), rendered);
    assertFalse(rendered.contains(":orders"), rendered);
    assertTrue(rendered.contains("orders"), rendered);

    OperationContext other =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate("urn:li:searchTemplate:other", 1L, "fulltext", null));
    String otherRendered =
        renderer.render(other, List.of(TestEntitySpecBuilder.getSpec()), "orders", true).toString();
    assertTrue(otherRendered.contains("orders-other"), otherRendered);
    assertFalse(otherRendered.contains("1.1"), otherRendered);
  }

  @Test
  public void testKnnInheritsUnlessCleared() {
    SearchTemplateProperties fulltext = body("Fulltext", "{ \"match_all\": {} }", null);
    fulltext.setKnn(
        "{ \"field\": \"embeddings.vector\", \"k\": 50, \"keyword_weight\": 0.5, \"vector_weight\": 2.0 }");
    store(SEARCH_TEMPLATE_FULLTEXT_URN, fulltext);
    SearchTemplateProperties child = parentOf("Child", SEARCH_TEMPLATE_FULLTEXT_URN);
    child.setFunctionScore(
        "{ \"functions\": [], \"score_mode\": \"multiply\", \"boost_mode\": \"multiply\" }");
    store("urn:li:searchTemplate:child", child);
    MergedSearchTemplate inherited = loader.load(opContext, "urn:li:searchTemplate:child");
    assertTrue(inherited.getKnn().contains("embeddings.vector"));
    assertTrue(inherited.getFunctionScore().contains("multiply"));
    assertTrue(inherited.getQuery().contains("match_all"));

    SearchTemplateProperties keywordOnly = parentOf("Keyword", SEARCH_TEMPLATE_FULLTEXT_URN);
    keywordOnly.setKnn(" ");
    store("urn:li:searchTemplate:keywordOnly", keywordOnly);
    assertNull(loader.load(opContext, "urn:li:searchTemplate:keywordOnly").getKnn());
    assertTrue(
        loader
            .load(opContext, "urn:li:searchTemplate:keywordOnly")
            .getQuery()
            .contains("match_all"));

    EntityIndexVersionConfiguration indexV3 = new EntityIndexVersionConfiguration();
    indexV3.setEnabled(true);
    indexV3.setKeywordReadEnabled(true);
    indexV3.setSemanticReadEnabled(true);
    ElasticSearchConfiguration elastic = v3Config();
    elastic.getEntityIndex().setV3(indexV3);
    elastic.getSearch().getV3().setTemplates(templates);
    SearchTemplateCoordinator semanticCoordinator =
        new SearchTemplateCoordinator(elastic, null, loader);
    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:child"));
    assertThrows(
        SearchTemplateException.class,
        () ->
            new SearchTemplateCoordinator(v3Config(), null, loader)
                .bind(opContext, "customer orders"));
    OperationContext semanticBound =
        semanticCoordinator.bind(opContext, "customer orders", false, List.of("document"));
    assertEquals(
        semanticBound.getSearchContext().getResolvedSearchTemplate().getUrn(),
        "urn:li:searchTemplate:child");

    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:keywordOnly"));
    OperationContext keywordBound = semanticCoordinator.bind(opContext, "customer orders");
    assertFalse(keywordBound.getSearchContext().getResolvedSearchTemplate().isKnnApplied());
    String key = keywordBound.getSearchContext().getResolvedSearchTemplate().cacheKeyComponent();
    keywordBound.getSearchContext().getResolvedSearchTemplate().setKnn(true, 1f, 1f, 100);
    assertEquals(
        keywordBound.getSearchContext().getResolvedSearchTemplate().cacheKeyComponent(), key);
    assertFalse(key.endsWith("@knn"));
  }

  @Test
  public void testContractSectionsCapabilitiesAndPlaceholders() {
    SearchTemplateProperties mergeNext = body("Merge", "{ \"match_all\": {} }", null);
    mergeNext.setLadder(parallelLadder("merge", "urn:li:searchTemplate:fulltext", false));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(mergeNext, 1024));

    SearchTemplateProperties anyNext = body("Any", "{ \"match_all\": {} }", null);
    anyNext.setLadder(parallelLadder("any", "urn:li:searchTemplate:fulltext", false));
    SearchTemplateChecks.check(anyNext, 1024);

    SearchTemplateProperties unknown = body("Rank", "{ \"match_all\": {} }", null);
    SearchTemplateRanking badPreset = new SearchTemplateRanking();
    badPreset.setPreset("popularity");
    unknown.setRanking(badPreset);
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(unknown, 1024));

    SearchTemplateLadder keywordFacets = parallelLadder("merge", null, true);
    keywordFacets.setFacets("candidates");
    SearchTemplateProperties facets = body("Facets", "{ \"match_all\": {} }", null);
    facets.setLadder(keywordFacets);
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(facets, 1024));

    SearchTemplateRanking supported = new SearchTemplateRanking();
    supported.setPreset("none");
    supported.setWindowSizer("fixed");
    SearchTemplateProperties none = body("None", "{ \"match_all\": {} }", null);
    none.setRanking(supported);
    SearchTemplateChecks.check(none, 1024);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, none);
    SearchTemplateCoordinator coordinator = new SearchTemplateCoordinator(v3Config(), null, loader);
    assertNotNull(
        coordinator
            .bind(opContext, "customer orders")
            .getSearchContext()
            .getResolvedSearchTemplate());

    SearchTemplateProperties usage = body("Usage", "{ \"match_all\": {} }", null);
    SearchTemplateRanking usageRanking = new SearchTemplateRanking();
    usageRanking.setPreset("usage");
    usage.setRanking(usageRanking);
    SearchTemplateChecks.check(usage, 1024);
    store("urn:li:searchTemplate:usage", usage);
    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:usage"));
    assertThrows(
        SearchTemplateException.class, () -> coordinator.bind(opContext, "customer orders"));
    templates.getDefaults().put("fulltext", Map.of("default", SEARCH_TEMPLATE_FULLTEXT_URN));

    HighlightFieldOverrides overrides = new HighlightFieldOverrides();
    overrides.setReplace(new StringArray("onlyThis"));
    none.setHighlightFields(overrides);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, none);
    expire();
    OperationContext highlighted =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    assertEquals(
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(
                highlighted,
                List.of(TestEntitySpecBuilder.getSpec()),
                "customer orders",
                true,
                null,
                false)
            .highlights(),
        List.of("onlyThis"));

    SearchTemplateProperties placeholders =
        body(
            "Placeholders",
            """
            {
              "bool": {
                "should": [
                  { "match": { "name": "{{raw}}" } },
                  { "match": { "name": "{{fuzzy}}" } },
                  { "match": { "name": "{{unquoted}}" } }
                  {{#flags.alphanumericSplit}}
                  ,{ "match": { "name": "{{alphanumericSplit}}" } }
                  {{/flags.alphanumericSplit}}
                ]
              }
            }
            """,
            null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, placeholders);
    expire();
    OperationContext shaped =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    String rendered =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
            .render(shaped, List.of(TestEntitySpecBuilder.getSpec()), "orders2017", true)
            .toString();
    assertTrue(rendered.contains("orders2017"), rendered);
    assertTrue(rendered.contains("orders 2017"), rendered);
  }

  @Test
  public void testFunctionScoreFilterSnippetMustParse() {
    String score =
        """
        {
          "functions": [
            { "filter": { "terms": { "domains": {{&domainsJson}} } }, "weight": 1.5 }
          ],
          "score_mode": "multiply",
          "boost_mode": "multiply"
        }
        """;
    assertThrows(
        SearchTemplateException.class,
        () -> SearchTemplateChecks.check(body("Domains", "{ \"match_all\": {} }", score), 1024));
    String broken =
        """
        {
          "functions": [
            { "filter": { "match": "orders" }, "weight": 1.5 }
          ],
          "score_mode": "multiply",
          "boost_mode": "multiply"
        }
        """;
    assertThrows(
        SearchTemplateException.class,
        () -> SearchTemplateChecks.check(body("Broken", "{ \"match_all\": {} }", broken), 1024));
  }

  @Test
  public void testFieldOverrideMergesWithParentAndReplaceDiscardsIt() {
    String parentUrn = "urn:li:searchTemplate:parentFields";
    SearchTemplateProperties parent = body("Parent", "{ \"match_all\": {} }", null);
    SearchFieldOverrides parentFields = new SearchFieldOverrides();
    SearchFieldBoost added = new SearchFieldBoost();
    added.setFieldName("description");
    added.setBoost(2.0);
    parentFields.setAdd(new SearchFieldBoostArray(added));
    parent.setFields(parentFields);
    store(parentUrn, parent);

    SearchTemplateProperties child = parentOf("Child", parentUrn);
    SearchFieldOverrides childFields = new SearchFieldOverrides();
    SearchFieldBoost weight = new SearchFieldBoost();
    weight.setFieldName("description");
    weight.setBoost(9.0);
    childFields.setOverride(new SearchFieldBoostArray(weight));
    child.setFields(childFields);
    MergedSearchTemplate merged =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childFields", child);
    assertEquals(merged.getFields().getAdd().get(0).getBoost(), 2.0);
    assertEquals(merged.getFields().getOverride().get(0).getBoost(), 9.0);

    SearchFieldOverrides replaced = new SearchFieldOverrides();
    SearchFieldBoost only = new SearchFieldBoost();
    only.setFieldName("name");
    only.setBoost(1.0);
    replaced.setReplace(new SearchFieldBoostArray(only));
    child.setFields(replaced);
    MergedSearchTemplate replacement =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childFields", child);
    assertFalse(replacement.getFields().hasAdd());
    assertEquals(replacement.getFields().getReplace().size(), 1);
    assertEquals(replacement.getFields().getReplace().get(0).getFieldName(), "name");
  }

  @Test
  public void testEmptyBoostsDropInheritedList() {
    String parentUrn = "urn:li:searchTemplate:parentBoosts";
    SearchTemplateProperties parent = body("Parent", "{ \"match_all\": {} }", null);
    SearchTemplateBoost boost = new SearchTemplateBoost();
    boost.setField("domains");
    boost.setValues(SearchTemplateViewer.DOMAIN_LIST);
    boost.setWeight(1.5);
    parent.setBoosts(new SearchTemplateBoostArray(boost));
    store(parentUrn, parent);

    SearchTemplateProperties child = parentOf("Child", parentUrn);
    child.setBoosts(new SearchTemplateBoostArray());
    MergedSearchTemplate merged =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childBoosts", child);
    assertNull(merged.getBoosts());
  }
}
