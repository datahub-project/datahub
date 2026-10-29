package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.CORP_USER_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_EXACT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_IDENTITY_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LIST_BY_URN_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LIST_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_SEARCH_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_STRUCTURED_EXACT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_STRUCTURED_URN;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.SearchTemplateValidator;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import com.linkedin.search.template.HighlightFieldOverrides;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateLadderStop;
import com.linkedin.search.template.SearchTemplateLadderStopArray;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRoute;
import com.linkedin.search.template.SearchTemplateRouteArray;
import com.linkedin.search.template.StructuredPropertyClause;
import com.linkedin.search.template.StructuredPropertyQuery;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.testng.annotations.BeforeMethod;

abstract class SearchTemplateFixture {
  protected static final Urn SYSTEM = UrnUtils.getUrn(SYSTEM_ACTOR);
  protected static final Urn USER = UrnUtils.getUrn("urn:li:corpuser:tester");
  protected static final String HOLE_QUERY =
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
  protected static final String FILLED_HOLE = "{ \"match_all\": {} }";
  protected static final String FULLTEXT_QUERY =
      """
      {
        "bool": {
          "should": [
            {
              "simple_query_string": {
                "query": "{{query}}",
                "default_operator": "AND",
                "fields": {{&defaultQueryFieldsJson}}
              }
            }
          ],
          "minimum_should_match": 1
        }
      }
      """;
  protected static final String LIST_SCORE =
      """
      {
        "functions": [
          { "filter": { "exists": { "field": "activeIncidents_urn" } }, "weight": 2.0 }
        ],
        "score_mode": "multiply",
        "boost_mode": "replace"
      }
      """;

  protected final Map<Urn, SearchTemplateProperties> stored = new HashMap<>();
  protected final Map<Urn, Long> writtenAt = new HashMap<>();
  protected final Map<Urn, StructuredPropertyDefinition> definitions = new HashMap<>();
  protected CachingAspectRetriever retriever;
  protected OperationContext opContext;
  protected final AtomicLong clock = new AtomicLong();
  protected SearchTemplateLoader loader;
  protected SearchTemplateConfiguration templates;
  protected EntityRegistry registry;

  @BeforeMethod
  public void setup() {
    stored.clear();
    writtenAt.clear();
    definitions.clear();
    registry = TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();
    retriever = mock(CachingAspectRetriever.class);
    when(retriever.getEntityRegistry()).thenReturn(registry);
    when(retriever.getLatestAspectObject(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Urn urn = invocation.getArgument(1);
              String aspect = invocation.getArgument(2);
              if (STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(aspect)) {
                StructuredPropertyDefinition definition = definitions.get(urn);
                return definition == null ? null : new Aspect(definition.data().copy());
              }
              SearchTemplateProperties properties = stored.get(urn);
              return properties == null ? null : new Aspect(properties.data().copy());
            });
    when(retriever.getLatestSystemAspect(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Urn urn = invocation.getArgument(1);
              String aspect = invocation.getArgument(2);
              if (!SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME.equals(aspect)) {
                return null;
              }
              SearchTemplateProperties properties = stored.get(urn);
              if (properties == null) {
                return null;
              }
              return SearchTemplateFixtures.systemAspect(
                  urn, properties, writtenAt.getOrDefault(urn, 1L));
            });
    when(retriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              Set<String> aspects = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                Map<String, Aspect> byAspect = new HashMap<>();
                if (aspects.contains(CORP_USER_KEY_ASPECT_NAME)
                    && "corpuser".equals(urn.getEntityType())) {
                  byAspect.put(
                      CORP_USER_KEY_ASPECT_NAME,
                      new Aspect(
                          new CorpUserInfo()
                              .setActive(true)
                              .setEmail(urn.getId())
                              .setDisplayName(urn.getId())
                              .data()));
                }
                for (String aspect : aspects) {
                  if (!STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(aspect)) {
                    continue;
                  }
                  StructuredPropertyDefinition definition = definitions.get(urn);
                  if (definition != null) {
                    byAspect.put(aspect, new Aspect(definition.data().copy()));
                  }
                }
                result.put(urn, byAspect);
              }
              return result;
            });
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(retriever)
            .cachingAspectRetriever(retriever)
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(structuredProperties())
            .build();
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(retrieverContext)
            .withSearchFlags(flags -> flags.setFulltext(true));
    templates = new SearchTemplateConfiguration();
    templates.setEnabled(true);
    templates.setMaxLeafClauses(8);
    SearchTemplateFixtures.setShippedLimits(templates);
    templates
        .getDefaults()
        .put("fulltext", new HashMap<>(Map.of("default", SEARCH_TEMPLATE_FULLTEXT_URN)));
    templates
        .getDefaults()
        .put("structured", new HashMap<>(Map.of("default", SEARCH_TEMPLATE_STRUCTURED_URN)));
    clock.set(0L);
    loader = new SearchTemplateLoader(600, clock::get);
  }

  protected void expire() {
    clock.addAndGet(600_000L + 1);
  }

  protected SearchTemplateWalk.Served<SearchResult> walk(
      String urn,
      String query,
      List<SortCriterion> sort,
      Integer size,
      Function<String, SearchResult> pages) {
    return walk(ForkJoinPool.commonPool(), urn, query, sort, size, pages);
  }

  protected SearchTemplateWalk.Served<SearchResult> walk(
      ExecutorService executor,
      String urn,
      String query,
      List<SortCriterion> sort,
      Integer size,
      Function<String, SearchResult> pages) {
    return SearchTemplateWalk.walk(
        opContext,
        loader,
        executor,
        urn,
        new SearchTemplateWalk.Request(query, null, sort, size, SearchTemplateWalk.identity()),
        templateUrn -> {
          try {
            SearchResult page = pages.apply(templateUrn);
            return SearchTemplateWalk.AttemptResult.ok(
                page,
                page.getNumEntities(),
                new ArrayList<>(page.getEntities()),
                List.of(),
                false,
                null);
          } catch (RuntimeException e) {
            return SearchTemplateWalk.AttemptResult.failed(e);
          }
        },
        (first, entities, total) -> {
          SearchResult copy = new SearchResult();
          copy.setEntities(new SearchEntityArray(entities));
          copy.setMetadata(first.getMetadata());
          copy.setFrom(first.getFrom());
          copy.setPageSize(first.getPageSize());
          copy.setNumEntities(total);
          return copy;
        });
  }

  protected static SearchResult page(int total, String... urns) {
    SearchResult result = new SearchResult();
    SearchEntityArray entities = new SearchEntityArray();
    for (String urn : urns) {
      entities.add(new SearchEntity().setEntity(UrnUtils.getUrn(urn)).setScore(1d));
    }
    result.setEntities(entities);
    result.setMetadata(new SearchResultMetadata());
    result.setFrom(0);
    result.setPageSize(10);
    result.setNumEntities(total);
    return result;
  }

  protected static SearchTemplateLadder parallelLadder(String join, String next, boolean withStop) {
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setParallel(
        new UrnArray(
            List.of(
                UrnUtils.getUrn("urn:li:searchTemplate:a"),
                UrnUtils.getUrn("urn:li:searchTemplate:b"))));
    ladder.setJoin(join);
    if (next != null) {
      ladder.setNext(UrnUtils.getUrn(next));
    }
    if (withStop) {
      SearchTemplateLadderStop stop = new SearchTemplateLadderStop();
      stop.setMinHits(1);
      ladder.setStop(new SearchTemplateLadderStopArray(stop));
    }
    return ladder;
  }

  protected static SearchTemplateLadder stopLadder(int minHits, String next) {
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    SearchTemplateLadderStop stop = new SearchTemplateLadderStop();
    stop.setMinHits(minHits);
    ladder.setStop(new SearchTemplateLadderStopArray(stop));
    if (next != null) {
      ladder.setNext(UrnUtils.getUrn(next));
    }
    return ladder;
  }

  protected SearchTemplateCoordinator useRuntime(@Nullable SearchTemplateCoordinator coordinator) {
    opContext = opContext.withSearchTemplateRuntime(coordinator);
    return coordinator;
  }

  protected static List<String> routeChain(OperationContext context, String query) {
    return SearchTemplateRuntimes.coordinator(context)
        .bind(context, query)
        .getSearchContext()
        .getResolvedSearchTemplate()
        .getRouteChain();
  }

  protected ElasticSearchConfiguration v3Config() {
    EntityIndexVersionConfiguration indexV3 = new EntityIndexVersionConfiguration();
    indexV3.setEnabled(true);
    indexV3.setKeywordReadEnabled(true);
    EntityIndexConfiguration index = new EntityIndexConfiguration();
    index.setV3(indexV3);
    SearchV3Configuration searchV3 = new SearchV3Configuration();
    searchV3.setTemplates(templates);
    SearchConfiguration search = new SearchConfiguration();
    search.setV3(searchV3);
    search.setMaxTermBucketSize(50);
    ElasticSearchConfiguration elastic = new ElasticSearchConfiguration();
    elastic.setEntityIndex(index);
    elastic.setSearch(search);
    return elastic;
  }

  protected void store(String urn, SearchTemplateProperties properties) {
    store(urn, properties, 1L);
  }

  protected void store(String urn, SearchTemplateProperties properties, long modifiedAt) {
    Urn parsed = UrnUtils.getUrn(urn);
    stored.put(parsed, properties);
    writtenAt.put(parsed, modifiedAt);
  }

  protected static DataHubPolicyInfo domainPolicy(
      String state, PolicyMatchCondition condition, String... values) {
    PolicyMatchCriterion criterion = new PolicyMatchCriterion();
    criterion.setField("DOMAIN");
    criterion.setValues(new StringArray(List.of(values)));
    criterion.setCondition(condition);
    return new DataHubPolicyInfo()
        .setDisplayName("policy")
        .setDescription("policy")
        .setType("METADATA")
        .setState(state)
        .setPrivileges(new StringArray("VIEW_ENTITY_PAGE"))
        .setActors(new DataHubActorFilter())
        .setResources(
            new DataHubResourceFilter()
                .setFilter(
                    new PolicyMatchFilter().setCriteria(new PolicyMatchCriterionArray(criterion))));
  }

  protected Urn installCertification(String version) {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    StructuredPropertyDefinition defined =
        searchableProperty("certification.status", "string", "Certification Status");
    if (version != null) {
      defined.setVersion(version);
    }
    definitions.put(property, defined);
    return property;
  }

  protected static StructuredPropertyDefinition searchableProperty(
      String qualifiedName, String valueType, String displayName) {
    StructuredPropertyDefinition defined = definition(qualifiedName, valueType, null);
    defined.setDisplayName(displayName);
    DataHubSearchConfig config = new DataHubSearchConfig();
    config.setQueryByDefault(true);
    config.setBoostScore(3.0);
    defined.setSearchConfiguration(config);
    return defined;
  }

  protected static SearchTemplateProperties nameValueTemplate(String functionScore) {
    SearchTemplateProperties template = body("Fulltext", "{ \"match_all\": {} }", functionScore);
    template.setStructuredPropertyQuery(valueAndName(8f));
    return template;
  }

  protected static StructuredPropertyQuery nameAndValue(float weight) {
    StructuredPropertyQuery section = new StructuredPropertyQuery();
    StructuredPropertyClause clause = new StructuredPropertyClause();
    clause.setWeight(weight);
    section.setNameAndValue(clause);
    return section;
  }

  protected static StructuredPropertyQuery valueAndName(float weight) {
    StructuredPropertyQuery section = nameAndValue(weight);
    section.setAnalyzedValue(new StructuredPropertyClause());
    section.setExactValue(new StructuredPropertyClause());
    return section;
  }

  protected String renderStored(String query, boolean scroll) {
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    return new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
        .render(
            bound, List.of(registry.getEntitySpec(DATASET_ENTITY_NAME)), query, true, null, scroll)
        .query()
        .toString();
  }

  protected SearchRequest searchRequest(
      OperationContext context, ElasticSearchConfiguration elastic, String query) {
    return SearchRequestHandler.getBuilder(
            context,
            TestEntitySpecBuilder.getSpec(),
            elastic,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG)
        .getSearchRequest(context, query, null, null, 0, 10, List.of());
  }

  protected static SearchResponse parseSearchResponse(String json) throws Exception {
    try (XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(
                SearchClientShimUtil.X_CONTENT_REGISTRY,
                LoggingDeprecationHandler.INSTANCE,
                json)) {
      return SearchResponse.fromXContent(parser);
    }
  }

  protected static StructuredPropertyDefinition definition(
      String qualifiedName, String valueType, String version) {
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setQualifiedName(qualifiedName);
    definition.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub." + valueType));
    definition.setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    if (version != null) {
      definition.setVersion(version);
    }
    return definition;
  }

  protected static final String ROUTE_QUERY =
      """
      {
        "bool": {
          "should": [
            {
              "simple_query_string": {
                "query": "{{query}}",
                "default_operator": "AND"
              }
            }
            {{#synonymPriority}}
            , { "term": { "name": { "value": "synonym", "boost": 1.5 } } }
            {{/synonymPriority}}
          ],
          "minimum_should_match": 1
        }
      }
      """;

  protected static HighlightFieldOverrides highlights(String field) {
    HighlightFieldOverrides overrides = new HighlightFieldOverrides();
    overrides.setAdd(new StringArray(field));
    return overrides;
  }

  protected SearchRequest searchRequest(
      OperationContext context, ElasticSearchConfiguration elastic, String query, int size) {
    return SearchRequestHandler.getBuilder(
            context,
            TestEntitySpecBuilder.getSpec(),
            elastic,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG)
        .getSearchRequest(context, query, null, null, 0, size, List.of("textFieldOverride"));
  }

  protected static void assertReservedFacets(SearchRequest request, int cap) {
    TermsAggregationBuilder entityType = termsAggregation(request, "_entityType");
    assertEquals(entityType.field(), "_entityType");
    assertEquals(entityType.minDocCount(), 0);
    assertEquals(entityType.size(), cap);
    TermsAggregationBuilder caller = termsAggregation(request, "textFieldOverride");
    assertNotEquals(caller.field(), "stolen");
    assertEquals(caller.size(), cap);
    assertEquals(
        request.source().aggregations().getAggregatorFactories().stream()
            .filter(aggregation -> "_entityType".equals(aggregation.getName()))
            .count(),
        1);
  }

  protected static TermsAggregationBuilder termsAggregation(SearchRequest request, String name) {
    return request.source().aggregations().getAggregatorFactories().stream()
        .filter(aggregation -> name.equals(aggregation.getName()))
        .map(TermsAggregationBuilder.class::cast)
        .findFirst()
        .orElseThrow();
  }

  protected void installShippedRoutes() {
    SearchTemplateProperties search = body("Search", null, null);
    search.setRoutes(searchRoutes());
    search.setDefaultRoute(defaultTo(SEARCH_TEMPLATE_FULLTEXT_URN));
    store(SEARCH_TEMPLATE_SEARCH_URN, search);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, pinned("Fulltext", ROUTE_QUERY));
    store(SEARCH_TEMPLATE_LIST_URN, pinned("List", "{ \"match_all\": {} }"));
    store(SEARCH_TEMPLATE_LIST_BY_URN_URN, pinned("List by URN", "{ \"match_all\": {} }"));
    store(SEARCH_TEMPLATE_EXACT_URN, pinned("Exact", "{ \"match_all\": {} }"));
    store(SEARCH_TEMPLATE_IDENTITY_URN, pinned("Identity", "{ \"match_all\": {} }"));
    SearchTemplateProperties structured =
        body("Structured", "{ \"term\": { \"name\": \"{{query}}\" } }", null);
    structured.setRoutes(structuredRoutes());
    store(SEARCH_TEMPLATE_STRUCTURED_URN, structured);
    SearchTemplateProperties structuredExact =
        parentOf("Structured exact", SEARCH_TEMPLATE_STRUCTURED_URN);
    structuredExact.setRoutes(new SearchTemplateRouteArray());
    store(SEARCH_TEMPLATE_STRUCTURED_EXACT_URN, structuredExact);
    SearchTemplateProperties postgres = body("Postgres", "to_tsquery('simple', '{{query}}')", null);
    postgres.setDialect(SearchTemplateDialect.POSTGRES);
    store("urn:li:searchTemplate:fulltextPostgres", postgres);
    templates
        .getDefaults()
        .put("fulltext", new HashMap<>(Map.of("default", SEARCH_TEMPLATE_SEARCH_URN)));
  }

  protected static SearchTemplateRouteArray searchRoutes() {
    return routes(
        route(
            "(?i)^(?:urn:li:[a-z][a-z0-9]*[:()].*|(?:s3|gs|hdfs):\\\\?/\\\\?/.*)",
            SEARCH_TEMPLATE_IDENTITY_URN),
        route("^\\*?$", SEARCH_TEMPLATE_LIST_BY_URN_URN),
        route("^\\\\/q [\"'].+[\"']$", SEARCH_TEMPLATE_STRUCTURED_EXACT_URN),
        route("^\\\\/q .*", SEARCH_TEMPLATE_STRUCTURED_URN),
        route("^[\"'].+[\"']$", SEARCH_TEMPLATE_EXACT_URN),
        route("^\\S+$", SEARCH_TEMPLATE_EXACT_URN));
  }

  protected static SearchTemplateRouteArray structuredRoutes() {
    return routes(
        route(
            "(?i)^(?:urn:li:[a-z][a-z0-9]*[:()].*|(?:s3|gs|hdfs):\\\\?/\\\\?/.*)",
            SEARCH_TEMPLATE_IDENTITY_URN),
        route("^\\*?$", SEARCH_TEMPLATE_LIST_URN),
        route("^(?:\\\\/q )?[\"'].+[\"']$", SEARCH_TEMPLATE_STRUCTURED_EXACT_URN));
  }

  protected static SearchTemplateProperties pinned(String name, String query) {
    SearchTemplateProperties properties = body(name, query, null);
    properties.setRoutes(new SearchTemplateRouteArray());
    return properties;
  }

  protected static SearchTemplateProperties bodyWithRoute(String regex, String target) {
    SearchTemplateProperties properties = body("Routed", "{ \"match_all\": {} }", null);
    properties.setRoutes(routes(route(regex, target)));
    return properties;
  }

  protected static SearchTemplateRoute route(String regex, String urn) {
    SearchTemplateRoute entry = new SearchTemplateRoute();
    entry.setQueryRegex(regex);
    entry.setTemplate(UrnUtils.getUrn(urn));
    return entry;
  }

  protected static SearchTemplateRouteArray routes(SearchTemplateRoute... entries) {
    return new SearchTemplateRouteArray(List.of(entries));
  }

  protected static SearchTemplateDefaultRoute defaultTo(String urn) {
    SearchTemplateDefaultRoute route = new SearchTemplateDefaultRoute();
    route.setTemplate(UrnUtils.getUrn(urn));
    return route;
  }

  protected static SearchTemplateProperties body(String name, String query, String functionScore) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(name);
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    if (query != null) {
      properties.setQuery(query);
    }
    if (functionScore != null) {
      properties.setFunctionScore(functionScore);
    }
    return properties;
  }

  protected static SearchTemplateProperties parentOf(String name, String parent) {
    SearchTemplateProperties properties = body(name, null, null);
    properties.setParent(UrnUtils.getUrn(parent));
    return properties;
  }

  protected List<AspectValidationException> validate(
      SearchTemplateValidator validator, RetrieverContext retrieverContext, ChangeMCP item) {
    List<AspectValidationException> exceptions =
        validator
            .validateProposed(opContext, List.of(item), retrieverContext, null)
            .collect(Collectors.toCollection(ArrayList::new));
    validator
        .validatePreCommit(opContext, List.of(item), retrieverContext)
        .forEach(exceptions::add);
    return exceptions;
  }

  protected SearchTemplateValidator templateValidator() {
    SearchConfiguration search = new SearchConfiguration();
    SearchV3Configuration v3 = new SearchV3Configuration();
    v3.setTemplates(templates);
    search.setV3(v3);
    return new SearchTemplateValidator(loader, search)
        .setConfig(
            AspectPluginConfig.builder()
                .className(SearchTemplateValidator.class.getName())
                .enabled(true)
                .supportedOperations(
                    List.of("CREATE", "CREATE_ENTITY", "DELETE", "UPDATE", "UPSERT", "PATCH"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(SEARCH_TEMPLATE_ENTITY_NAME)
                            .aspectName("*")
                            .build()))
                .build());
  }

  protected RetrieverContext holeContext() {
    return RetrieverContext.builder()
        .graphRetriever(GraphRetriever.EMPTY)
        .searchRetriever(childrenByParent())
        .cachingAspectRetriever(retriever)
        .build();
  }

  protected SearchRetriever childrenByParent() {
    return (entities, filters, scrollId, count, sortCriteria, searchFlags) -> {
      String parent = parentFilter(filters);
      SearchEntityArray found = new SearchEntityArray();
      if (parent != null) {
        for (Map.Entry<Urn, SearchTemplateProperties> entry : stored.entrySet()) {
          SearchTemplateProperties properties = entry.getValue();
          if (properties.hasParent()
              && properties.getParent() != null
              && parent.equals(properties.getParent().toString())) {
            found.add(new SearchEntity().setEntity(entry.getKey()));
          }
        }
      }
      ScrollResult result = new ScrollResult();
      result.setEntities(found);
      result.setNumEntities(found.size());
      result.setPageSize(found.size());
      return result;
    };
  }

  protected SearchRetriever structuredProperties() {
    return (entities, filters, scrollId, count, sortCriteria, searchFlags) -> {
      SearchEntityArray found = new SearchEntityArray();
      if (entities.contains(STRUCTURED_PROPERTY_ENTITY_NAME)) {
        definitions.keySet().stream()
            .sorted(Comparator.comparing(Urn::toString))
            .limit(count)
            .forEach(urn -> found.add(new SearchEntity().setEntity(urn)));
      }
      return new ScrollResult()
          .setEntities(found)
          .setNumEntities(found.size())
          .setPageSize(found.size());
    };
  }

  protected static String parentFilter(Filter filters) {
    if (filters == null || !filters.hasOr() || filters.getOr() == null) {
      return null;
    }
    for (ConjunctiveCriterion conjunction : filters.getOr()) {
      if (!conjunction.hasAnd() || conjunction.getAnd() == null) {
        continue;
      }
      for (Criterion criterion : conjunction.getAnd()) {
        if ("parent".equals(criterion.getField())
            && criterion.hasValues()
            && criterion.getValues() != null
            && !criterion.getValues().isEmpty()) {
          return criterion.getValues().get(0);
        }
      }
    }
    return null;
  }

  protected RetrieverContext retrieverContext() {
    return RetrieverContext.builder()
        .graphRetriever(GraphRetriever.EMPTY)
        .searchRetriever(SearchRetriever.EMPTY)
        .cachingAspectRetriever(retriever)
        .build();
  }

  protected static AspectPluginConfig pluginConfig(String className, List<String> operations) {
    return AspectPluginConfig.builder()
        .className(className)
        .enabled(true)
        .supportedOperations(operations)
        .supportedEntityAspectNames(
            List.of(
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName(SEARCH_TEMPLATE_ENTITY_NAME)
                    .aspectName(SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME)
                    .build()))
        .build();
  }

  protected static ChangeMCP change(Urn urn, SearchTemplateProperties properties) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getAspectName()).thenReturn(SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getAuditStamp()).thenReturn(new AuditStamp().setTime(5L).setActor(SYSTEM));
    when(item.getRecordTemplate()).thenReturn(properties);
    when(item.getMetadataChangeProposal()).thenReturn(null);
    return item;
  }

  protected static ChangeMCP item(
      Urn urn, Urn actor, ChangeType changeType, SearchTemplateProperties properties) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getAspectName()).thenReturn(SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
    when(item.getChangeType()).thenReturn(changeType);
    when(item.getAuditStamp()).thenReturn(new AuditStamp().setTime(5L).setActor(actor));
    when(item.getRecordTemplate()).thenReturn(properties);
    when(item.getMetadataChangeProposal()).thenReturn(null);
    return item;
  }
}
