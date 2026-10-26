package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LIST_BY_URN_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LIST_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_SEARCH_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_STRUCTURED_URN;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.SessionActorIdentity;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.HighlightFields;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ExplainMatch;
import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.PreparedSearchRequest;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchHitMatches;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchTemplateRequestApplier;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.search.template.HighlightFieldOverrides;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateBoost;
import com.linkedin.search.template.SearchTemplateBoostArray;
import com.linkedin.search.template.SearchTemplateProperties;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.core.CountRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.testng.annotations.Test;

public class SearchTemplateRequestFlowTest extends SearchTemplateFixture {
  @Test
  public void testBoundTemplateKeepsItsFulltextSelectionThroughFlagDefaults() {
    installShippedRoutes();
    ElasticSearchConfiguration elastic = v3Config();
    elastic.getSearch().getV3().setTemplates(templates);
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());

    OperationContext unset =
        opContext.withSearchFlags(flags -> new SearchFlags()).withSearchSurface(null);
    OperationContext structured =
        coordinator.bind(unset, "orders", false, List.of(DATASET_ENTITY_NAME));
    assertFalse(structured.getSearchContext().getSearchFlags().isFulltext());
    assertFalse(
        SearchUtils.applyDefaultSearchFlags(
                structured.getSearchContext().getSearchFlags(),
                "orders",
                new SearchFlags().setFulltext(true))
            .isFulltext());

    OperationContext fulltext =
        coordinator.bind(opContext, "orders", false, List.of(DATASET_ENTITY_NAME));
    assertTrue(fulltext.getSearchContext().getSearchFlags().isFulltext());
  }

  @Test
  public void testTemplateQueryCannotBypassSoftDeleteFilter() {
    SearchTemplateProperties properties =
        body("Fulltext", "{ \"term\": { \"removed\": true } }", null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, properties);
    ElasticSearchConfiguration elastic = v3Config();
    elastic.getSearch().getV3().setTemplates(templates);
    useRuntime(
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty()));

    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            opContext,
            TestEntitySpecBuilder.getSpec(),
            elastic,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    String hidden =
        handler
            .getSearchRequest(opContext, "orders", null, null, 0, 10, List.of())
            .source()
            .query()
            .toString();
    assertTrue(hidden.contains("\"value\" : true"), hidden);
    assertTrue(hidden.contains("\"must_not\""), hidden);
    assertEquals(occurrences(hidden, "\"removed\""), 2);

    OperationContext includingDeleted =
        opContext.withSearchFlags(flags -> flags.setIncludeSoftDeleted(true));
    String visible =
        handler
            .getSearchRequest(includingDeleted, "orders", null, null, 0, 10, List.of())
            .source()
            .query()
            .toString();
    assertEquals(occurrences(visible, "\"removed\""), 1);
  }

  @Test
  public void testSearchRequestKeepsSystemFiltersHighlightingAndTemplateSections() {
    SearchTemplateProperties properties = body("Fulltext", "{ \"match_all\": {} }", null);
    properties.setAggregations(
        "{ \"platform\": { \"terms\": { \"field\": \"platform.keyword\", \"size\": 20 } }, \"_entityType\": { \"terms\": { \"field\": \"_entityType\", \"size\": 1 } } }");
    properties.setSort("[ { \"name\": { \"order\": \"asc\" } } ]");
    properties.setFunctionScore(
        "{ \"functions\": [], \"score_mode\": \"multiply\", \"boost_mode\": \"multiply\" }");
    store(SEARCH_TEMPLATE_FULLTEXT_URN, properties);

    ElasticSearchConfiguration elastic = v3Config();
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    useRuntime(coordinator);
    OperationContext searching =
        opContext.withSearchFlags(
            flags ->
                flags.setFulltext(true).setSkipAggregates(false).setIncludeDefaultFacets(true));
    SearchRequest request =
        SearchRequestHandler.getBuilder(
                searching,
                TestEntitySpecBuilder.getSpec(),
                elastic,
                null,
                QueryFilterRewriteChain.EMPTY,
                TEST_SEARCH_SERVICE_CONFIG)
            .getSearchRequest(
                searching,
                "customer orders",
                null,
                null,
                0,
                10,
                List.of("platform", "_entityType"));
    String query = request.source().query().toString();
    assertTrue(query.contains("match_all"), query);
    assertTrue(query.contains("removed"), query);
    // This template names no highlight fields. An empty highlighter is omitted.
    assertNull(request.source().highlighter());
    List<String> aggregationNames =
        request.source().aggregations().getAggregatorFactories().stream()
            .map(aggregation -> aggregation.getName())
            .collect(Collectors.toList());
    assertEquals(aggregationNames.stream().filter("platform"::equals).count(), 1);
    assertEquals(aggregationNames.stream().filter("_entityType"::equals).count(), 1);
    assertTrue(request.source().rescores() == null || request.source().rescores().isEmpty());
    assertTrue(
        request.source().sorts().stream()
            .anyMatch(
                sort ->
                    sort instanceof FieldSortBuilder field && "name".equals(field.getFieldName())));
    assertTrue(
        request.source().sorts().stream()
            .anyMatch(
                sort ->
                    sort instanceof FieldSortBuilder field && "urn".equals(field.getFieldName())));

    SearchTemplateProperties rescored = body("Fulltext", "{ \"match_all\": {} }", null);
    rescored.setAggregations(
        "{ \"platform\": { \"terms\": { \"field\": \"platform.keyword\", \"size\": 20 } } }");
    rescored.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} }, \"query_weight\": 1.0, \"rescore_query_weight\": 1.0 } }");
    store(SEARCH_TEMPLATE_FULLTEXT_URN, rescored);
    expire();
    SearchRequest rescoredRequest =
        SearchRequestHandler.getBuilder(
                searching,
                TestEntitySpecBuilder.getSpec(),
                elastic,
                null,
                QueryFilterRewriteChain.EMPTY,
                TEST_SEARCH_SERVICE_CONFIG)
            .getSearchRequest(searching, "customer orders", null, null, 0, 10, List.of());
    assertFalse(rescoredRequest.source().rescores().isEmpty());
    SearchRequest scoreSort =
        SearchRequestHandler.getBuilder(
                searching,
                TestEntitySpecBuilder.getSpec(),
                elastic,
                null,
                QueryFilterRewriteChain.EMPTY,
                TEST_SEARCH_SERVICE_CONFIG)
            .getSearchRequest(
                searching,
                "customer orders",
                null,
                List.of(new SortCriterion().setField("_score").setOrder(SortOrder.DESCENDING)),
                0,
                10,
                List.of());
    assertFalse(scoreSort.source().rescores().isEmpty());
    assertEquals(rescoredRequest.source().sorts().size(), 2);
    assertTrue(rescoredRequest.source().sorts().get(0) instanceof ScoreSortBuilder);
    assertEquals(
        ((ScoreSortBuilder) rescoredRequest.source().sorts().get(0)).order(),
        org.opensearch.search.sort.SortOrder.DESC);
    assertTrue(rescoredRequest.source().sorts().get(1) instanceof FieldSortBuilder);
    assertEquals(
        ((FieldSortBuilder) rescoredRequest.source().sorts().get(1)).getFieldName(), "urn");
    assertEquals(
        ((FieldSortBuilder) rescoredRequest.source().sorts().get(1)).order(),
        org.opensearch.search.sort.SortOrder.ASC);
    SearchRequest countOnly =
        SearchRequestHandler.getBuilder(
                searching,
                TestEntitySpecBuilder.getSpec(),
                elastic,
                null,
                QueryFilterRewriteChain.EMPTY,
                TEST_SEARCH_SERVICE_CONFIG)
            .getSearchRequest(searching, "customer orders", null, null, 0, 0, List.of("platform"));
    assertNull(countOnly.source().highlighter());
    assertTrue(countOnly.source().rescores() == null || countOnly.source().rescores().isEmpty());
    assertTrue(
        countOnly.source().aggregations().getAggregatorFactories().stream()
            .anyMatch(aggregation -> "platform".equals(aggregation.getName())));

    SearchRequest callerSort =
        SearchRequestHandler.getBuilder(
                searching,
                TestEntitySpecBuilder.getSpec(),
                elastic,
                null,
                QueryFilterRewriteChain.EMPTY,
                TEST_SEARCH_SERVICE_CONFIG)
            .getSearchRequest(
                searching,
                "customer orders",
                null,
                List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
                0,
                10,
                List.of());
    assertFalse(
        callerSort.source().sorts().stream()
            .anyMatch(
                sort ->
                    sort instanceof FieldSortBuilder field && "name".equals(field.getFieldName())));
    assertTrue(callerSort.source().rescores() == null || callerSort.source().rescores().isEmpty());

    OperationContext skipAggregates =
        searching.withSearchFlags(flags -> flags.setSkipAggregates(true));
    SearchRequest skipped =
        SearchRequestHandler.getBuilder(
                skipAggregates,
                TestEntitySpecBuilder.getSpec(),
                elastic,
                null,
                QueryFilterRewriteChain.EMPTY,
                TEST_SEARCH_SERVICE_CONFIG)
            .getSearchRequest(
                skipAggregates, "customer orders", null, null, 0, 10, List.of("platform"));
    assertTrue(skipped.source().aggregations() == null);

    OperationContext callerFacetsOnly =
        searching.withSearchFlags(flags -> flags.setIncludeDefaultFacets(false));
    SearchRequest callerOnly =
        SearchRequestHandler.getBuilder(
                callerFacetsOnly,
                TestEntitySpecBuilder.getSpec(),
                elastic,
                null,
                QueryFilterRewriteChain.EMPTY,
                TEST_SEARCH_SERVICE_CONFIG)
            .getSearchRequest(
                callerFacetsOnly,
                "customer orders",
                null,
                null,
                0,
                10,
                List.of("textFieldOverride"));
    List<String> callerNames =
        callerOnly.source().aggregations().getAggregatorFactories().stream()
            .map(aggregation -> aggregation.getName())
            .collect(Collectors.toList());
    assertEquals(callerNames, List.of("textFieldOverride"));
  }

  @Test
  public void testFieldConfigurationAndCustomHighlights() {
    String removed = "description";
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", FULLTEXT_QUERY, null));
    CustomSearchConfiguration custom =
        CustomSearchConfiguration.builder()
            .fieldConfigurations(
                Map.of(
                    "narrow",
                    FieldConfiguration.builder()
                        .searchFields(SearchFields.builder().remove(List.of(removed)).build())
                        .highlightFields(HighlightFields.builder().remove(List.of(removed)).build())
                        .build()))
            .build();
    SearchTemplateRenderer renderer =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), custom);
    OperationContext labeled =
        opContext
            .withSearchFlags(flags -> flags.setFulltext(true).setFieldConfiguration("narrow"))
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    String rendered =
        renderer
            .render(labeled, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    // A fieldConfiguration flag names a custom-search label. Template queries do not read it.
    OperationContext unlabeled =
        opContext
            .withSearchFlags(flags -> flags.setFulltext(true))
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    assertEquals(
        rendered,
        renderer
            .render(unlabeled, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString());
    assertTrue(
        labeled.getSearchContext().getResolvedSearchTemplate().getHighlightFields().isEmpty());

    OperationContext customHighlights =
        labeled.withSearchFlags(
            flags -> flags.setCustomHighlightingFields(new StringArray("customHighlight")));
    assertEquals(
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(
                customHighlights,
                List.of(TestEntitySpecBuilder.getSpec()),
                "orders",
                true,
                null,
                false)
            .highlights(),
        List.of("customHighlight"));
  }

  @Test
  public void testV3KeywordReadOffIgnoresInstalledTemplates() {
    store(
        SEARCH_TEMPLATE_FULLTEXT_URN,
        body("Fulltext", "{ \"term\": { \"name\": \"from-template\" } }", null));
    ElasticSearchConfiguration elastic = v3Config();
    elastic.getEntityIndex().getV3().setKeywordReadEnabled(false);
    EntityIndexVersionConfiguration v2 = new EntityIndexVersionConfiguration();
    v2.setEnabled(true);
    elastic.getEntityIndex().setV2(v2);
    useRuntime(
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty()));
    SearchConfiguration search = new SearchConfiguration();
    search.setPartial(new PartialConfiguration());
    search.setExactMatch(new ExactMatchConfiguration());
    search.setWordGram(new WordGramConfiguration());
    String rendered =
        new SearchQueryBuilder(search, null)
            .buildQuery(opContext, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    assertFalse(rendered.contains("from-template"), rendered);
  }

  @Test
  public void testBindReplacesThePreviousResolution() {
    SearchTemplateProperties fulltext = body("Fulltext", FULLTEXT_QUERY, null);
    fulltext.setRoutes(routes(route("^\\*?$", SEARCH_TEMPLATE_LIST_URN)));
    store(SEARCH_TEMPLATE_FULLTEXT_URN, fulltext);
    store(SEARCH_TEMPLATE_LIST_URN, body("List", "{ \"match_all\": {} }", LIST_SCORE));
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            v3Config(),
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    OperationContext first = coordinator.bind(opContext, "customer orders");
    OperationContext second = coordinator.bind(first, "*");
    assertEquals(
        first.getSearchContext().getResolvedSearchTemplate().getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);
    assertEquals(
        second.getSearchContext().getResolvedSearchTemplate().getUrn(), SEARCH_TEMPLATE_LIST_URN);
    assertNull(second.getSearchContext().getSearchFlags().getSearchTemplate());
  }

  @Test
  public void testStampHookWritesApiVersionAndTheMergeStaysCached() {
    Urn custom = UrnUtils.getUrn("urn:li:searchTemplate:custom");
    SearchTemplateProperties properties = body("Custom", "{ \"match_all\": {} }", null);
    ChangeMCP change =
        ChangeItemImpl.builder()
            .urn(custom)
            .aspectName(SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .recordTemplate(properties)
            .auditStamp(new AuditStamp().setTime(5L).setActor(SYSTEM))
            .build(retriever);
    new SearchTemplateStampHook()
        .setConfig(
            pluginConfig(
                SearchTemplateStampHook.class.getName(),
                List.of("CREATE", "UPSERT", "UPDATE", "PATCH", "RESTATE")))
        .writeMutation(opContext, List.of(change), retrieverContext())
        .count();
    assertEquals(properties.getApiVersion().intValue(), SearchTemplateCapabilities.API_VERSION);

    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    assertTrue(
        loader.load(opContext, SEARCH_TEMPLATE_FULLTEXT_URN).getQuery().contains("match_all"));
    store(
        SEARCH_TEMPLATE_FULLTEXT_URN,
        body("Fulltext", "{ \"term\": { \"name\": \"next\" } }", null));
    assertTrue(
        loader.load(opContext, SEARCH_TEMPLATE_FULLTEXT_URN).getQuery().contains("match_all"));
  }

  @Test
  public void testViewerPlaceholderAndQueryByDefaultFields() {
    SearchTemplateProperties viewed =
        body("Viewed", "{ \"term\": { \"urn\": \"{{viewerUrn}}\" } }", null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, viewed);
    ElasticSearchConfiguration elastic = v3Config();
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    OperationContext bound = coordinator.bind(opContext, "customer orders");
    String key = bound.getSearchContext().getResolvedSearchTemplate().cacheKeyComponent();
    assertTrue(key.contains("@viewer=" + opContext.getSessionActorContext().getActorUrn()));
    String rendered =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    assertTrue(rendered.contains(opContext.getSessionActorContext().getActorUrn().toString()));

    SearchTemplateProperties plain = body("Fulltext", FULLTEXT_QUERY, null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, plain);
    expire();
    OperationContext plainBound = coordinator.bind(opContext, "customer orders");
    assertFalse(
        plainBound
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent()
            .contains("@viewer="));
    List<String> expected =
        SearchTemplateFields.analyzedFields(
            new V3SearchQueryBuilder(elastic.getSearch(), null)
                .searchedFieldConfigs(opContext, List.of(TestEntitySpecBuilder.getSpec())));
    String fields =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
            .render(plainBound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    for (String field : expected) {
      assertTrue(fields.contains(field), field);
    }
    assertFalse(fields.contains("textFieldOverride^1.0"), fields);
  }

  @Test
  public void testDomainsJsonIsInjectedAndCannotBeSpoofed() {
    SearchTemplateProperties scored = body("Fulltext", FULLTEXT_QUERY, null);
    SearchTemplateBoost boost = new SearchTemplateBoost();
    boost.setField("domains");
    boost.setValues(SearchTemplateViewer.DOMAIN_LIST);
    boost.setWeight(1.5);
    scored.setBoosts(new SearchTemplateBoostArray(boost));
    scored.setParameters(
        new StringMap(Map.of(SearchTemplateViewer.DOMAIN_LIST, "[\"urn:li:domain:spoof\"]")));
    store(SEARCH_TEMPLATE_FULLTEXT_URN, scored);
    ElasticSearchConfiguration elastic = v3Config();
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    OperationContext bound = coordinator.bind(opContext, "customer orders");
    assertTrue(
        bound
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent()
            .contains("@domains=[]"));
    String rendered =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    assertFalse(rendered.contains("spoof"));
    assertFalse(rendered.contains("1.5"));
    String weighted =
        SearchTemplateJson.wrapFunctionScore(
                QueryBuilders.matchAllQuery(),
                null,
                List.of(
                    new SearchTemplateJson.TermsBoost(
                        "domains", List.of("urn:li:domain:finance"), 1.5)),
                SearchTemplateJson.FunctionScoreTail.none())
            .toString();
    assertTrue(weighted.contains("1.5"));
    assertTrue(weighted.contains("urn:li:domain:finance"));
    assertTrue(weighted.contains("terms"));

    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", FULLTEXT_QUERY, null));
    expire();
    OperationContext plain = coordinator.bind(opContext, "customer orders");
    assertFalse(
        plain
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent()
            .contains("@domains="));
  }

  @Test
  public void testViewerDomainBoostChangesRenderedListAndCacheIdentity() {
    Urn finance = UrnUtils.getUrn("urn:li:domain:finance");
    when(retriever.getLatestAspectObject(any(), eq(USER), eq("domains")))
        .thenReturn(new Aspect(new Domains().setDomains(new UrnArray(finance)).data()));

    SearchTemplateProperties scored = body("List", "{ \"match_all\": {} }", null);
    SearchTemplateBoost boost = new SearchTemplateBoost();
    boost.setField("domains_urn");
    boost.setValues(SearchTemplateViewer.DOMAINS);
    boost.setWeight(1.25);
    scored.setBoosts(new SearchTemplateBoostArray(boost));
    store(SEARCH_TEMPLATE_FULLTEXT_URN, scored);

    OperationContext financeViewer =
        opContext.asSession(
            RequestContext.TEST,
            Authorizer.EMPTY,
            new Authentication(new Actor(ActorType.USER, USER.getId()), ""));
    Urn other = UrnUtils.getUrn("urn:li:corpuser:other");
    OperationContext otherViewer =
        opContext.asSession(
            RequestContext.TEST,
            Authorizer.EMPTY,
            new Authentication(new Actor(ActorType.USER, other.getId()), ""));
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            v3Config(),
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    OperationContext financeBound = coordinator.bind(financeViewer, "customer orders");
    OperationContext otherBound = coordinator.bind(otherViewer, "customer orders");

    String financeKey =
        financeBound.getSearchContext().getResolvedSearchTemplate().cacheKeyComponent();
    String otherKey = otherBound.getSearchContext().getResolvedSearchTemplate().cacheKeyComponent();
    assertTrue(financeKey.contains("@domains=[\"urn:li:domain:finance\"]"), financeKey);
    assertTrue(otherKey.contains("@domains=[]"), otherKey);
    assertFalse(financeKey.equals(otherKey));

    SearchTemplateRenderer renderer =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null);
    String financeQuery =
        renderer
            .render(financeBound, List.of(TestEntitySpecBuilder.getSpec()), "customer orders", true)
            .toString();
    String otherQuery =
        renderer
            .render(otherBound, List.of(TestEntitySpecBuilder.getSpec()), "customer orders", true)
            .toString();
    assertTrue(financeQuery.contains("domains_urn"), financeQuery);
    assertTrue(financeQuery.contains("urn:li:domain:finance"), financeQuery);
    assertTrue(financeQuery.contains("1.25"), financeQuery);
    assertFalse(otherQuery.contains("1.25"), otherQuery);
  }

  @Test
  public void testShippedListRendersViewerOwnerAndGroupBoosts() {
    installShippedRoutes();
    Map<String, Object> searchLight = new HashMap<>();
    searchLight.put("default", "urn:li:searchTemplate:searchLight");
    templates.setSurfaces(
        Map.of(SearchTemplateSurfaces.SEARCH_ENTITIES, Map.of("fulltext", searchLight)));
    Urn team = UrnUtils.getUrn("urn:li:corpGroup:rankTeam");
    Authorizer authorizer =
        new Authorizer() {
          @Override
          public Optional<SessionActorIdentity> resolveSessionActorIdentity(Urn actorUrn) {
            return Optional.of(new SessionActorIdentity(actorUrn, List.of(team), Set.of()));
          }

          @Override
          public List<Urn> getActorGroups(Urn actorUrn) {
            return List.of(team);
          }
        };
    OperationContext viewer =
        OperationContext.asSession(
                opContext,
                RequestContext.TEST,
                authorizer,
                new Authentication(new Actor(ActorType.USER, USER.getId()), ""),
                false)
            .withSearchFlags(flags -> flags.setFulltext(true))
            .withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES);
    for (String templateUrn : SearchTemplateBootstrap.urns()) {
      store(templateUrn, SearchTemplateBootstrap.get(templateUrn));
    }
    SearchTemplateLoader storage = new SearchTemplateLoader(0, SearchTemplateSource.STORAGE);
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            v3Config(),
            null,
            storage,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    OperationContext bound = coordinator.bind(viewer, "*");
    assertEquals(
        bound.getSearchContext().getResolvedSearchTemplate().getUrn(), SEARCH_TEMPLATE_LIST_URN);
    MergedSearchTemplate list = storage.load(bound, SEARCH_TEMPLATE_LIST_URN);
    assertNotNull(list.getFunctionScore());
    assertFalse(list.isClasspathBridge());
    assertNotNull(list.getBoosts());
    String rendered =
        new SearchTemplateRenderer(storage, new SearchConfiguration(), null)
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "*", true)
            .toString();

    assertTrue(rendered.contains(USER.toString()), rendered);
    assertTrue(rendered.contains(team.toString()), rendered);
    assertTrue(rendered.contains("owners_urn"), rendered);
    String key = bound.getSearchContext().getResolvedSearchTemplate().cacheKeyComponent();
    assertTrue(key.contains("@viewer=" + USER), key);
    assertTrue(key.contains("@groups=[\"" + team + "\"]"), key);
  }

  @Test
  public void testDomainListFromPoliciesKeepsSeeds() {
    Urn finance = UrnUtils.getUrn("urn:li:domain:finance");
    Urn child = UrnUtils.getUrn("urn:li:domain:payments");
    Set<Urn> urns =
        SearchTemplateViewer.domainUrnsFromPolicies(
            List.of(
                domainPolicy(
                    PoliciesConfig.ACTIVE_POLICY_STATE,
                    PolicyMatchCondition.EQUALS,
                    finance.toString(),
                    "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"),
                domainPolicy(
                    PoliciesConfig.ACTIVE_POLICY_STATE,
                    PolicyMatchCondition.NOT_EQUALS,
                    "urn:li:domain:other"),
                domainPolicy(
                    PoliciesConfig.ACTIVE_POLICY_STATE,
                    PolicyMatchCondition.STARTS_WITH,
                    "urn:li:domain:fin"),
                domainPolicy(
                    PoliciesConfig.INACTIVE_POLICY_STATE,
                    PolicyMatchCondition.EQUALS,
                    "urn:li:domain:closed"),
                new DataHubPolicyInfo()
                    .setDisplayName("all")
                    .setDescription("all")
                    .setType("METADATA")
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setPrivileges(new StringArray("VIEW_ENTITY_PAGE"))
                    .setActors(new DataHubActorFilter())));
    assertEquals(urns, Set.of(finance));
    Set<Urn> expanded = SearchTemplateViewer.includeSeeds(Set.of(finance), seed -> Set.of(child));
    assertTrue(expanded.contains(finance));
    assertTrue(expanded.contains(child));
  }

  @Test
  public void testAggregateAndBrowseSurfacesResolveCompleteQueries() {
    SearchTemplateConfiguration previous = templates;
    SearchTemplateConfiguration shipped = new SearchTemplateConfiguration();
    shipped.setEnabled(true);
    SearchTemplateFixtures.setShippedLimits(shipped);
    Map<String, Object> search = new HashMap<>();
    search.put("default", SEARCH_TEMPLATE_SEARCH_URN);
    Map<String, Object> structured = new HashMap<>();
    structured.put("default", SEARCH_TEMPLATE_STRUCTURED_URN);
    shipped.setDefaults(Map.of("fulltext", search, "structured", structured));
    shipped.setSurfaces(
        Map.of(
            SearchTemplateSurfaces.AGGREGATE_ENTITIES,
            Map.of("fulltext", search),
            SearchTemplateSurfaces.SCROLL_ENTITIES,
            Map.of("fulltext", search)));
    templates = shipped;
    try {
      ElasticSearchConfiguration elastic = v3Config();
      SearchTemplateLoader classpath = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
      useRuntime(
          new SearchTemplateCoordinator(
              elastic,
              null,
              classpath,
              ForkJoinPool.commonPool(),
              SearchTemplateComponentRegistry.empty()));

      OperationContext aggregate =
          opContext.withSearchSurface(SearchTemplateSurfaces.AGGREGATE_ENTITIES);
      assertEquals(
          routeChain(aggregate, "*"),
          List.of(SEARCH_TEMPLATE_SEARCH_URN, SEARCH_TEMPLATE_LIST_BY_URN_URN));
      assertEquals(
          routeChain(aggregate, "orders"),
          List.of(SEARCH_TEMPLATE_SEARCH_URN, SEARCH_TEMPLATE_FULLTEXT_URN));
      assertFalse(routeChain(aggregate, "orders").contains("urn:li:searchTemplate:light"));

      OperationContext browsing =
          opContext.withSearchSurface(null).withResolvedSearchTemplate(null);
      assertEquals(
          routeChain(browsing, "*"),
          List.of(SEARCH_TEMPLATE_SEARCH_URN, SEARCH_TEMPLATE_LIST_BY_URN_URN));
      assertEquals(
          routeChain(browsing, "orders"),
          List.of(SEARCH_TEMPLATE_SEARCH_URN, SEARCH_TEMPLATE_FULLTEXT_URN));
      assertNull(classpath.load(opContext, SEARCH_TEMPLATE_LIST_BY_URN_URN).getFunctionScore());
      assertNotNull(classpath.load(opContext, SEARCH_TEMPLATE_FULLTEXT_URN).getFunctionScore());

      SearchRequestHandler handler =
          SearchRequestHandler.getBuilder(
              browsing,
              TestEntitySpecBuilder.getSpec(),
              elastic,
              null,
              QueryFilterRewriteChain.EMPTY,
              TEST_SEARCH_SERVICE_CONFIG);
      String star = handler.getQuery(browsing, "*", true).toString();
      assertTrue(star.contains("match_all"), star);
      assertFalse(star.contains("function_score"), star);
      String keyword = handler.getQuery(browsing, "orders", true).toString();
      assertTrue(keyword.contains("simple_query_string"), keyword);
      assertTrue(keyword.contains("function_score"), keyword);

      OperationContext counting =
          aggregate.withSearchFlags(
              flags ->
                  flags.setFulltext(true).setSkipAggregates(false).setIncludeDefaultFacets(true));
      SearchRequest request =
          SearchRequestHandler.getBuilder(
                  counting,
                  TestEntitySpecBuilder.getSpec(),
                  elastic,
                  null,
                  QueryFilterRewriteChain.EMPTY,
                  TEST_SEARCH_SERVICE_CONFIG)
              .getSearchRequest(counting, "orders", null, null, 0, 0, List.of("_entityType"));
      assertNull(request.source().highlighter());
      assertTrue(request.source().rescores() == null || request.source().rescores().isEmpty());
      assertTrue(
          request.source().aggregations().getAggregatorFactories().stream()
              .anyMatch(aggregation -> "_entityType".equals(aggregation.getName())));
    } finally {
      templates = previous;
      useRuntime(null);
    }
  }

  @Test
  public void testHighlightFieldsAreApplied() {
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    SearchTemplateProperties template = body("Highlights", "{ \"match_all\": {} }", null);
    HighlightFieldOverrides highlights = new HighlightFieldOverrides();
    highlights.setAdd(new StringArray("tags_1"));
    template.setHighlightFields(highlights);
    store("urn:li:searchTemplate:highlights", template);
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate("urn:li:searchTemplate:highlights", 1L, "fulltext", null));
    assertTrue(
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true, null, false)
            .highlights()
            .contains("tags_1"));

    OperationContext adjusted =
        bound.withSearchFlags(
            flags -> {
              HighlightFieldOverrides request = new HighlightFieldOverrides();
              request.setRemove(new StringArray("tags_1"));
              request.setAdd(new StringArray("name.ngram"));
              return flags.setHighlightFields(request);
            });
    assertEquals(
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(adjusted, List.of(TestEntitySpecBuilder.getSpec()), "orders", true, null, false)
            .highlights(),
        List.of("name"));

    OperationContext override =
        bound.withSearchFlags(
            flags -> flags.setCustomHighlightingFields(new StringArray("description.delimited")));
    assertEquals(
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(override, List.of(TestEntitySpecBuilder.getSpec()), "orders", true, null, false)
            .highlights(),
        List.of("description"));

    OperationContext skipped = override.withSearchFlags(flags -> flags.setSkipHighlighting(true));
    assertTrue(
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(skipped, List.of(TestEntitySpecBuilder.getSpec()), "orders", true, null, false)
            .highlights()
            .isEmpty());
  }

  @Test
  public void testUnsetHighlightFieldsHighlightNothing() {
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
        .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true);
    assertTrue(bound.getSearchContext().getResolvedSearchTemplate().getHighlightFields().isEmpty());
  }

  @Test
  public void testChildHighlightRemoveChangesTheParentSet() {
    String parentUrn = "urn:li:searchTemplate:parentHighlights";
    SearchTemplateProperties parent = body("Parent", "{ \"match_all\": {} }", null);
    HighlightFieldOverrides parentHighlights = new HighlightFieldOverrides();
    parentHighlights.setAdd(new StringArray("name", "description"));
    parent.setHighlightFields(parentHighlights);
    store(parentUrn, parent);

    SearchTemplateProperties child = parentOf("Child", parentUrn);
    HighlightFieldOverrides childHighlights = new HighlightFieldOverrides();
    childHighlights.setRemove(new StringArray("name"));
    child.setHighlightFields(childHighlights);
    MergedSearchTemplate merged =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childHighlights", child);
    assertTrue(merged.getHighlightFields().getAdd().contains("description"));
    assertFalse(merged.getHighlightFields().getAdd().contains("name"));

    SearchTemplateProperties cleared = parentOf("Cleared", parentUrn);
    cleared.setHighlightFields(new HighlightFieldOverrides());
    assertNull(
        loader
            .preview(opContext, retriever, "urn:li:searchTemplate:clearedHighlights", cleared)
            .getHighlightFields());
  }

  @Test
  public void testHighlightNameIsNotExpanded() {
    SearchTemplateProperties template = body("Highlights", "{ \"match_all\": {} }", null);
    HighlightFieldOverrides highlights = new HighlightFieldOverrides();
    highlights.setAdd(new StringArray("tags_1"));
    template.setHighlightFields(highlights);
    store("urn:li:searchTemplate:literalHighlight", template);
    ElasticSearchConfiguration elastic = v3Config();
    useRuntime(
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty()));
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(
                "urn:li:searchTemplate:literalHighlight", 1L, "fulltext", null));
    SearchRequest request = searchRequest(bound, elastic, "orders");
    assertNull(request.source().highlighter());

    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    OperationContext empty =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    SearchRequest blank = searchRequest(empty, elastic, "orders");
    assertNull(blank.source().highlighter());
  }

  @Test
  public void testTemplateSearchReportsAMatchedFieldFromSource() throws Exception {
    SearchTemplateProperties template = body("Fulltext", "{ \"match_all\": {} }", null);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    SearchFieldBoost name = new SearchFieldBoost();
    name.setFieldName("name");
    name.setBoost(1.0);
    SearchFieldBoost tags = new SearchFieldBoost();
    tags.setFieldName("tags");
    tags.setBoost(1.0);
    SearchFieldBoostArray added = new SearchFieldBoostArray();
    added.add(name);
    added.add(tags);
    fields.setAdd(added);
    template.setFields(fields);
    HighlightFieldOverrides highlights = new HighlightFieldOverrides();
    highlights.setAdd(new StringArray("name"));
    template.setHighlightFields(highlights);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
    ElasticSearchConfiguration elastic = v3Config();
    useRuntime(
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty()));
    OperationContext searching =
        opContext
            .withSearchFlags(
                flags -> flags.setFulltext(true).setSkipHighlighting(false).setSkipAggregates(true))
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            searching,
            registry.getEntitySpec(DATASET_ENTITY_NAME),
            elastic,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    PreparedSearchRequest prepared =
        handler.prepareSearchRequest(
            searching, "orders", null, null, 0, 10, List.of(), null, false);
    SearchResponse response =
        parseSearchResponse(
            """
        {
          "took": 1,
          "timed_out": false,
          "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
          "hits": {
            "total": {"value": 1, "relation": "eq"},
            "max_score": 1.0,
            "hits": [{
              "_index": "test",
              "_id": "1",
              "_score": 1.0,
              "matched_queries": ["exactName"],
              "_source": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)",
                "name": "customer orders",
                "tags": ["finance"]
              }
            }]
          }
        }
        """);
    assertNull(prepared.request().source().highlighter());
    assertTrue(
        Arrays.stream(prepared.request().source().fetchSource().includes())
            .anyMatch(path -> "name".equals(path) || path.startsWith("name.")),
        Arrays.toString(prepared.request().source().fetchSource().includes()));
    SearchResult result =
        handler.extractResult(searching, response, null, 0, 10, prepared.template());
    assertEquals(result.getEntities().size(), 1);
    List<MatchedField> matched = result.getEntities().get(0).getMatchedFields();
    assertTrue(
        matched.stream()
            .anyMatch(
                field ->
                    "name".equals(field.getName()) && "customer orders".equals(field.getValue())),
        matched.toString());
    assertFalse(
        matched.stream().anyMatch(field -> "tags".equals(field.getName())), matched.toString());
    assertTrue(
        matched.stream().anyMatch(field -> "exactName".equals(field.getName())),
        matched.toString());
    assertFalse(result.getEntities().get(0).hasExplainMatches());

    OperationContext explaining = searching.withSearchFlags(flags -> flags.setIncludeExplain(true));
    SearchResult explained =
        handler.extractResult(explaining, response, null, 0, 10, prepared.template());
    ExplainMatch explainedName =
        explained.getEntities().get(0).getExplainMatches().stream()
            .filter(evidence -> "name".equals(evidence.getField()))
            .findFirst()
            .orElseThrow();
    assertEquals(explainedName.getValue(), "customer orders");
    assertEquals(explainedName.getSubfield(), "name.stemmed");
    assertEquals(explainedName.getMode(), "word");
    assertEquals(explainedName.getQuery(), "orders");
  }

  @Test
  public void testDatasetSearchRejectsKnnWhileScrollDoesNot() {
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    SearchTemplateProperties child = parentOf("Child", SEARCH_TEMPLATE_FULLTEXT_URN);
    child.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    store("urn:li:searchTemplate:knnChild", child);
    EntityIndexVersionConfiguration indexV3 = new EntityIndexVersionConfiguration();
    indexV3.setEnabled(true);
    indexV3.setKeywordReadEnabled(true);
    indexV3.setSemanticReadEnabled(true);
    ElasticSearchConfiguration elastic = v3Config();
    elastic.getEntityIndex().setV3(indexV3);
    elastic.getSearch().getV3().setTemplates(templates);
    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:knnChild"));
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    assertThrows(
        SearchTemplateException.class,
        () -> coordinator.bind(opContext, "customer orders", false, List.of("dataset")));
    OperationContext scrolled =
        coordinator.bind(opContext, "customer orders", true, List.of("dataset"));
    assertEquals(
        scrolled.getSearchContext().getResolvedSearchTemplate().getUrn(),
        "urn:li:searchTemplate:knnChild");

    SearchTemplateProperties deferredKnn = body("Deferred kNN", "{ \"match_all\": {} }", null);
    deferredKnn.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    store("urn:li:searchTemplate:deferredKnn", deferredKnn);
    SearchTemplateProperties keyword = body("Keyword", "{ \"match_all\": {} }", null);
    keyword.setLadder(stopLadder(1, "urn:li:searchTemplate:deferredKnn"));
    store("urn:li:searchTemplate:keyword", keyword);
    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:keyword"));
    OperationContext deferred =
        coordinator.bind(opContext, "customer orders", false, List.of("dataset"));
    assertEquals(
        deferred.getSearchContext().getResolvedSearchTemplate().getUrn(),
        "urn:li:searchTemplate:keyword");
  }

  @Test
  public void testCountOnlyKeepsLexicalQueryAndFacetsWithoutHitDecorations() {
    SearchTemplateProperties properties =
        body("Fulltext", "{ \"term\": { \"name\": \"{{query}}\" } }", null);
    properties.setHighlightFields(highlights("name"));
    properties.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} }, \"query_weight\": 1.0, \"rescore_query_weight\": 1.0 } }");
    properties.setAggregations(
        "{ \"platform\": { \"terms\": { \"field\": \"platform.keyword\", \"size\": 5000 } }, \"_entityType\": { \"terms\": { \"field\": \"stolen\", \"size\": 1 } }, \"textFieldOverride\": { \"terms\": { \"field\": \"stolen\", \"size\": 5000 } } }");
    store(SEARCH_TEMPLATE_FULLTEXT_URN, properties);
    ElasticSearchConfiguration elastic = v3Config();
    useRuntime(
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty()));
    OperationContext searching =
        opContext.withSearchFlags(
            flags ->
                flags
                    .setFulltext(true)
                    .setSkipAggregates(false)
                    .setIncludeDefaultFacets(true)
                    .setSkipHighlighting(false)
                    .setGetSuggestions(true)
                    .setMaxAggValues(7));
    try {
      SearchRequest hits = searchRequest(searching, elastic, "customer orders", 10);
      SearchRequest count = searchRequest(searching, elastic, "customer orders", 0);
      assertTrue(hits.source().query().toString().contains("customer orders"));
      assertTrue(count.source().query().toString().contains("customer orders"));
      assertNull(hits.source().highlighter());
      assertFalse(hits.source().rescores().isEmpty());
      assertNotNull(hits.source().suggest());
      assertNull(count.source().highlighter());
      assertTrue(count.source().rescores() == null || count.source().rescores().isEmpty());
      assertNull(count.source().suggest());
      assertReservedFacets(count, 7);
      assertEquals(termsAggregation(count, "platform").size(), 7);
      assertEquals(termsAggregation(count, "platform").field(), "platform.keyword");
    } finally {
      useRuntime(null);
    }
  }

  @Test
  public void testAggregateRequestKeepsJavaTermsAggregation() {
    installShippedRoutes();
    SearchTemplateProperties list =
        pinned("List", "{ \"term\": { \"templateMarker\": \"list-body\" } }");
    list.setHighlightFields(highlights("name"));
    list.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} }, \"query_weight\": 1.0, \"rescore_query_weight\": 1.0 } }");
    list.setAggregations(
        "{ \"platform\": { \"terms\": { \"field\": \"stolen\", \"size\": 5000 } }, \"groups\": { \"terms\": { \"field\": \"browsePathV2\", \"size\": 3 } } }");
    store(SEARCH_TEMPLATE_LIST_BY_URN_URN, list);
    expire();
    ElasticSearchConfiguration elastic = v3Config();
    useRuntime(
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty()));
    try {
      ESSearchDAO dao =
          new ESSearchDAO(
              false, elastic, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG);
      SearchRequest request =
          dao.buildAggregateByValue(opContext, List.of(DATASET_ENTITY_NAME), "platform", null, 10);
      String query = request.source().query().toString();
      assertTrue(query.contains("list-body"), query);
      assertTrue(query.contains("removed"), query);
      assertEquals(request.source().size(), 0);
      assertNull(request.source().highlighter());
      assertTrue(request.source().rescores() == null || request.source().rescores().isEmpty());
      assertNull(request.source().suggest());
      assertEquals(request.source().aggregations().getAggregatorFactories().size(), 1);
      TermsAggregationBuilder platform = termsAggregation(request, "platform");
      assertEquals(platform.size(), 10);
      assertFalse("stolen".equals(platform.field()));
    } finally {
      useRuntime(null);
    }
  }

  @Test
  public void testFilterCountAndAggregateRenderOneTemplate() {
    installShippedRoutes();
    store(
        SEARCH_TEMPLATE_LIST_URN,
        pinned("List", "{ \"term\": { \"templateMarker\": \"list-body\" } }"));
    store(
        SEARCH_TEMPLATE_LIST_BY_URN_URN,
        pinned("List by URN", "{ \"term\": { \"templateMarker\": \"complete-list-body\" } }"));
    store(
        "urn:li:searchTemplate:customList",
        pinned("Custom", "{ \"term\": { \"templateMarker\": \"custom-body\" } }"));
    ElasticSearchConfiguration elastic = v3Config();
    useRuntime(
        new SearchTemplateCoordinator(
            elastic,
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty()));
    ESSearchDAO dao =
        new ESSearchDAO(
            false, elastic, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG);

    String filterQuery =
        dao.buildFilterRequest(opContext, DATASET_ENTITY_NAME, null, null, 0, 10)
            .source()
            .query()
            .toString();
    String countQuery =
        dao.buildDocCountRequest(opContext, DATASET_ENTITY_NAME, null).query().toString();
    String occupancyCountQuery =
        dao.buildDocCountRequest(
                opContext.withSkipTemplateExecution(true), DATASET_ENTITY_NAME, null)
            .query()
            .toString();
    String aggregateQuery =
        dao.buildAggregateByValue(opContext, List.of(DATASET_ENTITY_NAME), "platform", null, 10)
            .source()
            .query()
            .toString();
    assertTrue(filterQuery.contains("complete-list-body"), filterQuery);
    assertTrue(countQuery.contains("complete-list-body"), countQuery);
    assertFalse(occupancyCountQuery.contains("list-body"), occupancyCountQuery);
    assertTrue(aggregateQuery.contains("complete-list-body"), aggregateQuery);
    assertTrue(
        dao
            .buildAggregateByValue(opContext, List.of(DATASET_ENTITY_NAME), "platform", null, 10)
            .source()
            .aggregations()
            .getAggregatorFactories()
            .stream()
            .anyMatch(aggregation -> "platform".equals(aggregation.getName())));

    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            opContext,
            TestEntitySpecBuilder.getSpec(),
            elastic,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    String searchQuery =
        handler
            .getSearchRequest(opContext, "*", null, null, 0, 10, List.of())
            .source()
            .query()
            .toString();
    String scrollQuery =
        handler
            .prepareSearchRequest(opContext, "*", null, null, 0, 10, List.of(), null, true)
            .request()
            .source()
            .query()
            .toString();
    String browseQuery = handler.getQuery(opContext, "*", true).toString();
    String lineageQuery =
        handler
            .getQuery(
                opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES), "*", true)
            .toString();
    String keywordQuery =
        handler
            .getSearchRequest(opContext, "customer orders", null, null, 0, 10, List.of())
            .source()
            .query()
            .toString();
    assertTrue(searchQuery.contains("list-body"), searchQuery);
    assertTrue(scrollQuery.contains("complete-list-body"), scrollQuery);
    assertTrue(browseQuery.contains("complete-list-body"), browseQuery);
    assertTrue(lineageQuery.contains("list-body"), lineageQuery);
    assertTrue(keywordQuery.contains("simple_query_string"), keywordQuery);
    assertTrue(keywordQuery.contains("customer orders"), keywordQuery);
    assertFalse(keywordQuery.contains("list-body"), keywordQuery);

    OperationContext explicit =
        opContext.withSearchFlags(
            flags -> flags.setSearchTemplate(UrnUtils.getUrn("urn:li:searchTemplate:customList")));
    String explicitQuery =
        dao.buildFilterRequest(explicit, DATASET_ENTITY_NAME, null, null, 0, 10)
            .source()
            .query()
            .toString();
    assertTrue(explicitQuery.contains("custom-body"), explicitQuery);
    assertFalse(explicitQuery.contains("list-body"), explicitQuery);

    useRuntime(null);
    String legacy =
        dao.buildFilterRequest(opContext, DATASET_ENTITY_NAME, null, null, 0, 10)
            .source()
            .query()
            .toString();
    CountRequest legacyCount = dao.buildDocCountRequest(opContext, DATASET_ENTITY_NAME, null);
    String legacyAggregate =
        dao.buildAggregateByValue(opContext, List.of(DATASET_ENTITY_NAME), "platform", null, 10)
            .source()
            .query()
            .toString();
    assertFalse(legacy.contains("templateMarker"), legacy);
    assertFalse(legacyCount.query().toString().contains("templateMarker"));
    assertFalse(legacyAggregate.contains("templateMarker"), legacyAggregate);
  }

  @Test
  public void testClauseBudgetStaysOnTheRenderedTemplate() {
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", FULLTEXT_QUERY, null));
    OperationContext bound =
        opContext
            .withSearchFlags(flags -> flags.setFulltext(true))
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    SearchV3QueryBuilder builder =
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null);
    RenderedSearchTemplate first =
        builder.render(
            bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true, null, false);
    RenderedSearchTemplate second =
        builder.render(
            bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true, null, false);
    assertTrue(first.estimatedClauses() > 0);
    assertEquals(second.estimatedClauses(), first.estimatedClauses());
  }

  @Test
  public void testRequestApplierAppliesTemplateSort() {
    SearchTemplateRequestApplier applier =
        new SearchTemplateRequestApplier(
            mock(AggregationQueryBuilder.class),
            25,
            List.of(TestEntitySpecBuilder.getSpec()),
            null,
            3,
            null);
    RenderedSearchTemplate template =
        new RenderedSearchTemplate(
            QueryBuilders.matchAllQuery(),
            null,
            null,
            List.of(new FieldSortBuilder("name.keyword")),
            List.of(),
            List.of(),
            "orders",
            Map.of(),
            12);
    SearchSourceBuilder source = new SearchSourceBuilder();
    SearchFlags flags = new SearchFlags().setSkipHighlighting(true).setSkipAggregates(true);
    applier.apply(opContext, source, flags, null, List.of(), template, false, 10, "orders");
    assertEquals(source.sorts().size(), 2);
    assertEquals(((FieldSortBuilder) source.sorts().get(0)).getFieldName(), "name.keyword");
    assertEquals(((FieldSortBuilder) source.sorts().get(1)).getFieldName(), "urn");
  }

  @Test
  public void testRequestApplierMatchesFieldsOnlyForQueryWords() {
    SearchTemplateRequestApplier applier =
        new SearchTemplateRequestApplier(
            mock(AggregationQueryBuilder.class),
            25,
            List.of(opContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME)),
            null,
            3,
            new V3SearchQueryBuilder(v3Config().getSearch(), null));
    SearchHit hit = new SearchHit(1);
    hit.sourceRef(
        new BytesArray(
            "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)\","
                + "\"name\":\"orders\"}"));

    Function<SearchHit, SearchHitMatches> words =
        applier.matchedFieldsOf(opContext, queriedNameTemplate("orders"));
    Function<SearchHit, SearchHitMatches> matchAll =
        applier.matchedFieldsOf(opContext, queriedNameTemplate("*"));

    assertTrue(
        words.apply(hit).matched().stream().anyMatch(field -> "name".equals(field.getName())),
        words.apply(hit).toString());
    assertTrue(matchAll.apply(hit).matched().isEmpty(), matchAll.apply(hit).toString());
    assertTrue(matchAll.apply(hit).explain().isEmpty(), matchAll.apply(hit).toString());
  }

  private static RenderedSearchTemplate queriedNameTemplate(String query) {
    return new RenderedSearchTemplate(
        QueryBuilders.matchAllQuery(),
        null,
        null,
        null,
        List.of("name"),
        List.of("name"),
        query,
        Map.of(),
        1);
  }

  private static int occurrences(String value, String needle) {
    return value.split(java.util.regex.Pattern.quote(needle), -1).length - 1;
  }
}
