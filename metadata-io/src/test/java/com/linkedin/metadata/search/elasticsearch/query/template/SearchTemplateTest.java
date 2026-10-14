package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_EXACT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_IDENTITY_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LINEAGE_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LIST_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_SEARCH_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_STRUCTURED_EXACT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_STRUCTURED_URN;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.SearchTemplateValidator;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.HighlightFields;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.PreparedSearchRequest;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.V3SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchTemplateJson;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchTemplateMustache;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.v3.StructuredPropertyQueryFields;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import com.linkedin.search.template.HighlightFieldOverrides;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateBoost;
import com.linkedin.search.template.SearchTemplateBoostArray;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateLadderStop;
import com.linkedin.search.template.SearchTemplateLadderStopArray;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRoute;
import com.linkedin.search.template.SearchTemplateRouteArray;
import com.linkedin.search.template.StructuredPropertyQuery;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.core.CountRequest;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchTemplateTest {
  private static final Urn SYSTEM = UrnUtils.getUrn(SYSTEM_ACTOR);
  private static final Urn USER = UrnUtils.getUrn("urn:li:corpuser:tester");
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
  private static final String FILLED_HOLE = "{ \"match_all\": {} }";
  private static final String FULLTEXT_QUERY =
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
  private static final String LIST_SCORE =
      """
      {
        "functions": [
          { "filter": { "term": { "hasActiveIncidents": { "value": true } } }, "weight": 2.0 }
        ],
        "score_mode": "multiply",
        "boost_mode": "replace"
      }
      """;

  private final Map<Urn, SearchTemplateProperties> stored = new HashMap<>();
  private final Map<Urn, StructuredPropertyDefinition> definitions = new HashMap<>();
  private CachingAspectRetriever retriever;
  private OperationContext opContext;
  private SearchTemplateLoader loader;
  private SearchTemplateConfiguration templates;
  private EntityRegistry registry;

  @BeforeMethod
  public void setup() {
    stored.clear();
    definitions.clear();
    SearchTemplates.install(null);
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
    when(retriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              Set<String> aspects = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                Map<String, Aspect> byAspect = new HashMap<>();
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
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(retriever)
            .withSearchFlags(flags -> flags.setFulltext(true));
    templates = new SearchTemplateConfiguration();
    templates.setEnabled(true);
    templates.setMaxLeafClauses(8);
    templates
        .getDefaults()
        .put("fulltext", new HashMap<>(Map.of("default", SEARCH_TEMPLATE_FULLTEXT_URN)));
    templates
        .getDefaults()
        .put("structured", new HashMap<>(Map.of("default", SEARCH_TEMPLATE_STRUCTURED_URN)));
    loader = new SearchTemplateLoader(600);
  }

  @AfterMethod
  public void clearTemplates() {
    SearchTemplates.install(null);
  }

  @Test
  public void testShippedRouteOrder() {
    installShippedRoutes();
    SearchTemplateSelector selector = new SearchTemplateSelector(templates, loader);
    assertEquals(
        selector
            .select(
                opContext, "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)", true, null)
            .getUrn(),
        SEARCH_TEMPLATE_IDENTITY_URN);
    assertEquals(
        selector.select(opContext, "s3://bucket/path/object", true, null).getUrn(),
        SEARCH_TEMPLATE_IDENTITY_URN);
    assertEquals(selector.select(opContext, "", true, null).getUrn(), SEARCH_TEMPLATE_LIST_URN);
    assertEquals(selector.select(opContext, "*", true, null).getUrn(), SEARCH_TEMPLATE_LIST_URN);
    assertEquals(
        selector.select(opContext, "\\/q \"orders\"", true, null).getUrn(),
        SEARCH_TEMPLATE_STRUCTURED_EXACT_URN);
    assertEquals(
        selector.select(opContext, "\\/q platform:snowflake", true, null).getUrn(),
        SEARCH_TEMPLATE_STRUCTURED_URN);
    assertEquals(
        selector.select(opContext, "\"orders\"", true, null).getUrn(), SEARCH_TEMPLATE_EXACT_URN);
    assertEquals(
        selector.select(opContext, "orders", true, null).getUrn(), SEARCH_TEMPLATE_EXACT_URN);
    ResolvedSearchTemplate fulltext = selector.select(opContext, "customer orders", true, null);
    assertEquals(fulltext.getUrn(), SEARCH_TEMPLATE_FULLTEXT_URN);
    assertEquals(
        fulltext.getRouteChain(),
        List.of(SEARCH_TEMPLATE_SEARCH_URN, SEARCH_TEMPLATE_FULLTEXT_URN));
    String rendered =
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(
                opContext.withResolvedSearchTemplate(fulltext),
                List.of(TestEntitySpecBuilder.getSpec()),
                "customer orders",
                true)
            .toString();
    assertTrue(rendered.contains("simple_query_string"), rendered);
    assertTrue(rendered.contains("customer"), rendered);

    assertEquals(
        selector.select(opContext, "\"orders\"", false, null).getUrn(),
        SEARCH_TEMPLATE_STRUCTURED_EXACT_URN);
    assertEquals(
        selector.select(opContext, "\\/q \"orders\"", false, null).getUrn(),
        SEARCH_TEMPLATE_STRUCTURED_EXACT_URN);
    assertEquals(
        selector.select(opContext, "orders", false, null).getUrn(), SEARCH_TEMPLATE_STRUCTURED_URN);
    assertEquals(
        selector.select(opContext, "\\/q platform:snowflake", false, null).getUrn(),
        SEARCH_TEMPLATE_STRUCTURED_URN);

    OperationContext lineage =
        opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH_ACROSS_LINEAGE);
    assertEquals(
        selector.select(lineage, "customer orders", true, null).getUrn(),
        SEARCH_TEMPLATE_LINEAGE_URN);
    assertEquals(selector.select(lineage, "*", true, null).getUrn(), SEARCH_TEMPLATE_LIST_URN);
    ResolvedSearchTemplate lineageResolved =
        selector.select(lineage, "customer orders", true, null);
    String lineageRendered =
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(
                lineage.withResolvedSearchTemplate(lineageResolved),
                List.of(TestEntitySpecBuilder.getSpec()),
                "customer orders",
                true)
            .toString();
    assertTrue(lineageRendered.contains("synonym"), lineageRendered);
  }

  @Test
  public void testSelection() {
    installShippedRoutes();
    SearchTemplateSelector selector = new SearchTemplateSelector(templates, loader);
    assertEquals(
        selector
            .select(
                opContext,
                "customer orders",
                true,
                SearchClientShim.SearchEngineType.ELASTICSEARCH_8)
            .getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);

    Map<String, Object> fulltextSlots = templates.getDefaults().get("fulltext");
    fulltextSlots.put("elasticsearch8", "urn:li:searchTemplate:fulltextPostgres");
    assertEquals(
        selector
            .select(
                opContext,
                "customer orders",
                true,
                SearchClientShim.SearchEngineType.ELASTICSEARCH_8)
            .getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);
    fulltextSlots.remove("elasticsearch8");

    OperationContext pinned =
        opContext.withSearchFlags(
            flags ->
                flags
                    .setFulltext(true)
                    .setSearchTemplate(UrnUtils.getUrn(SEARCH_TEMPLATE_LIST_URN)));
    ResolvedSearchTemplate selected = selector.select(pinned, "orders", true, null);
    assertEquals(selected.getUrn(), SEARCH_TEMPLATE_LIST_URN);
    assertEquals(selected.getRouteChain(), List.of(SEARCH_TEMPLATE_LIST_URN));

    OperationContext routed =
        opContext.withSearchFlags(
            flags ->
                flags
                    .setFulltext(true)
                    .setSearchTemplate(UrnUtils.getUrn(SEARCH_TEMPLATE_SEARCH_URN)));
    assertEquals(
        selector.select(routed, "urn:li:corpuser:alice", true, null).getUrn(),
        SEARCH_TEMPLATE_IDENTITY_URN);

    OperationContext postgresOverride =
        opContext.withSearchFlags(
            flags ->
                flags
                    .setFulltext(true)
                    .setSearchTemplate(UrnUtils.getUrn("urn:li:searchTemplate:fulltextPostgres")));
    assertThrows(
        SearchTemplateException.class,
        () -> selector.select(postgresOverride, "orders", true, null));

    templates.getDefaults().remove("fulltext");
    assertThrows(
        SearchTemplateException.class,
        () -> selector.select(opContext, "customer orders", true, null));
  }

  @Test
  public void testMissingEngineSeedFallsThrough() {
    installShippedRoutes();
    templates
        .getDefaults()
        .get("fulltext")
        .put("elasticsearch8", "urn:li:searchTemplate:missingEngine");
    SearchTemplateSelector selector = new SearchTemplateSelector(templates, loader);
    assertEquals(
        selector
            .select(
                opContext,
                "customer orders",
                true,
                SearchClientShim.SearchEngineType.ELASTICSEARCH_8)
            .getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);
  }

  @Test
  public void testRouteInheritance() {
    String parent = "urn:li:searchTemplate:router";
    SearchTemplateProperties parentBody =
        body("Router", "{ \"term\": { \"name\": \"parent\" } }", null);
    parentBody.setRoutes(routes(route("^\\*?$", SEARCH_TEMPLATE_LIST_URN)));
    store(parent, parentBody);
    store(SEARCH_TEMPLATE_LIST_URN, pinned("List", "{ \"match_all\": {} }"));

    SearchTemplateProperties inherited = parentOf("Inherited", parent);
    store("urn:li:searchTemplate:inherited", inherited);
    SearchTemplateSelector selector = new SearchTemplateSelector(templates, loader);
    OperationContext inheritedContext =
        opContext.withSearchFlags(
            flags -> flags.setSearchTemplate(UrnUtils.getUrn("urn:li:searchTemplate:inherited")));
    assertEquals(
        selector.select(inheritedContext, "*", true, null).getUrn(), SEARCH_TEMPLATE_LIST_URN);
    assertEquals(
        selector.select(inheritedContext, "orders", true, null).getUrn(),
        "urn:li:searchTemplate:inherited");

    SearchTemplateProperties replaced = parentOf("Replaced", parent);
    replaced.setRoutes(routes(route("^\\S+$", SEARCH_TEMPLATE_EXACT_URN)));
    store("urn:li:searchTemplate:replaced", replaced);
    store(SEARCH_TEMPLATE_EXACT_URN, pinned("Exact", "{ \"match_all\": {} }"));
    OperationContext replacedContext =
        opContext.withSearchFlags(
            flags -> flags.setSearchTemplate(UrnUtils.getUrn("urn:li:searchTemplate:replaced")));
    assertEquals(
        selector.select(replacedContext, "orders", true, null).getUrn(), SEARCH_TEMPLATE_EXACT_URN);
    assertEquals(
        selector.select(replacedContext, "customer orders", true, null).getUrn(),
        "urn:li:searchTemplate:replaced");

    SearchTemplateProperties dropped = parentOf("Dropped", parent);
    dropped.setRoutes(new SearchTemplateRouteArray());
    store("urn:li:searchTemplate:dropped", dropped);
    OperationContext droppedContext =
        opContext.withSearchFlags(
            flags -> flags.setSearchTemplate(UrnUtils.getUrn("urn:li:searchTemplate:dropped")));
    assertEquals(
        selector.select(droppedContext, "*", true, null).getUrn(), "urn:li:searchTemplate:dropped");
  }

  @Test
  public void testClearedDefaultRunsItsBody() {
    SearchTemplateProperties router = body("Router", null, null);
    router.setDefaultRoute(defaultTo(SEARCH_TEMPLATE_FULLTEXT_URN));
    store("urn:li:searchTemplate:router", router);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, pinned("Fulltext", FULLTEXT_QUERY));
    SearchTemplateProperties child = parentOf("Child", "urn:li:searchTemplate:router");
    child.setDefaultRoute(new SearchTemplateDefaultRoute());
    child.setQuery("{ \"term\": { \"name\": \"{{query}}\" } }");
    store("urn:li:searchTemplate:child", child);

    SearchTemplateSelector selector = new SearchTemplateSelector(templates, loader);
    OperationContext context =
        opContext.withSearchFlags(
            flags -> flags.setSearchTemplate(UrnUtils.getUrn("urn:li:searchTemplate:child")));
    ResolvedSearchTemplate resolved = selector.select(context, "customer orders", true, null);
    assertEquals(resolved.getUrn(), "urn:li:searchTemplate:child");
    String rendered =
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(
                context.withResolvedSearchTemplate(resolved),
                List.of(TestEntitySpecBuilder.getSpec()),
                "customer orders",
                true)
            .toString();
    assertTrue(rendered.contains("customer"), rendered);
  }

  @Test
  public void testRouteWriteRejection() {
    SearchTemplateProperties withQuery = body("Router", "{ \"match_all\": {} }", null);
    withQuery.setDefaultRoute(defaultTo(SEARCH_TEMPLATE_FULLTEXT_URN));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(withQuery, 1024));

    SearchTemplateProperties withLadder = body("Router", null, null);
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setSelect(new StringArray("scoreSort"));
    withLadder.setLadder(ladder);
    withLadder.setDefaultRoute(defaultTo(SEARCH_TEMPLATE_FULLTEXT_URN));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(withLadder, 1024));

    assertThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateChecks.check(
                bodyWithRoute("(?=orders)", SEARCH_TEMPLATE_LIST_URN), 1024));
    assertThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateChecks.check(
                bodyWithRoute(
                    "a".repeat(SearchTemplateRoutes.MAX_ROUTE_PATTERN_LENGTH + 1),
                    SEARCH_TEMPLATE_LIST_URN),
                1024));

    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", FULLTEXT_QUERY, null));
    SearchTemplateProperties parent = body("Parent", FULLTEXT_QUERY, null);
    store("urn:li:searchTemplate:parentBody", parent);
    SearchTemplateProperties child = body("Child", null, null);
    child.setParent(UrnUtils.getUrn("urn:li:searchTemplate:parentBody"));
    child.setDefaultRoute(defaultTo(SEARCH_TEMPLATE_FULLTEXT_URN));
    SearchTemplateValidator validator = templateValidator();
    assertFalse(
        validate(
                validator,
                holeContext(),
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:child"),
                    SYSTEM,
                    ChangeType.UPSERT,
                    child))
            .isEmpty());

    SearchTemplateProperties self = body("Self", "{ \"match_all\": {} }", null);
    self.setRoutes(routes(route("^orders$", "urn:li:searchTemplate:self")));
    assertFalse(
        validate(
                validator,
                holeContext(),
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:self"), SYSTEM, ChangeType.UPSERT, self))
            .isEmpty());

    SearchTemplateProperties missing = body("Missing", "{ \"match_all\": {} }", null);
    missing.setRoutes(routes(route("^orders$", "urn:li:searchTemplate:absent")));
    assertFalse(
        validate(
                validator,
                holeContext(),
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:missing"),
                    SYSTEM,
                    ChangeType.UPSERT,
                    missing))
            .isEmpty());
  }

  @Test
  public void testBatchSeesSiblingTemplates() {
    SearchTemplateProperties structured = body("Structured", "{ \"match_all\": {} }", null);
    structured.setRoutes(routes(route("^orders$", SEARCH_TEMPLATE_STRUCTURED_EXACT_URN)));
    SearchTemplateProperties structuredExact =
        parentOf("Structured exact", SEARCH_TEMPLATE_STRUCTURED_URN);
    structuredExact.setRoutes(new SearchTemplateRouteArray());
    SearchTemplateProperties fulltext = pinned("Fulltext", "{ \"match_all\": {} }");
    SearchTemplateProperties search = body("Search", null, null);
    search.setRoutes(routes(route("^\\S+$", SEARCH_TEMPLATE_STRUCTURED_URN)));
    search.setDefaultRoute(defaultTo(SEARCH_TEMPLATE_FULLTEXT_URN));
    SearchTemplateValidator validator = templateValidator();
    RetrieverContext empty = holeContext();
    assertTrue(
        validator
            .validateProposed(
                opContext,
                List.of(
                    item(
                        UrnUtils.getUrn(SEARCH_TEMPLATE_STRUCTURED_URN),
                        SYSTEM,
                        ChangeType.UPSERT,
                        structured),
                    item(
                        UrnUtils.getUrn(SEARCH_TEMPLATE_STRUCTURED_EXACT_URN),
                        SYSTEM,
                        ChangeType.UPSERT,
                        structuredExact),
                    item(
                        UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN),
                        SYSTEM,
                        ChangeType.UPSERT,
                        fulltext),
                    item(
                        UrnUtils.getUrn(SEARCH_TEMPLATE_SEARCH_URN),
                        SYSTEM,
                        ChangeType.UPSERT,
                        search)),
                empty,
                null)
            .collect(Collectors.toList())
            .isEmpty());

    SearchTemplateProperties absent = body("Search", null, null);
    absent.setDefaultRoute(defaultTo("urn:li:searchTemplate:absent"));
    assertFalse(
        validator
            .validateProposed(
                opContext,
                List.of(
                    item(
                        UrnUtils.getUrn(SEARCH_TEMPLATE_SEARCH_URN),
                        SYSTEM,
                        ChangeType.UPSERT,
                        absent)),
                empty,
                null)
            .collect(Collectors.toList())
            .isEmpty());

    SearchTemplateProperties left = parentOf("Left", "urn:li:searchTemplate:right");
    SearchTemplateProperties right = parentOf("Right", "urn:li:searchTemplate:left");
    assertFalse(
        validator
            .validateProposed(
                opContext,
                List.of(
                    item(
                        UrnUtils.getUrn("urn:li:searchTemplate:left"),
                        SYSTEM,
                        ChangeType.UPSERT,
                        left),
                    item(
                        UrnUtils.getUrn("urn:li:searchTemplate:right"),
                        SYSTEM,
                        ChangeType.UPSERT,
                        right)),
                empty,
                null)
            .collect(Collectors.toList())
            .isEmpty());
  }

  @Test
  public void testRouteBindFailuresAndIntern() {
    SearchTemplateProperties left = body("Left", null, null);
    left.setDefaultRoute(defaultTo("urn:li:searchTemplate:right"));
    SearchTemplateProperties right = body("Right", null, null);
    right.setDefaultRoute(defaultTo("urn:li:searchTemplate:left"));
    store("urn:li:searchTemplate:left", left);
    store("urn:li:searchTemplate:right", right);
    templates
        .getDefaults()
        .put("fulltext", new HashMap<>(Map.of("default", "urn:li:searchTemplate:left")));
    SearchTemplateSelector selector = new SearchTemplateSelector(templates, loader);
    assertThrows(
        SearchTemplateException.class,
        () -> selector.select(opContext, "customer orders", true, null));

    String previous = null;
    for (int i = 0; i <= SearchTemplateRoutes.MAX_ROUTE_HOPS; i++) {
      String urn = "urn:li:searchTemplate:hop" + i;
      SearchTemplateProperties hop =
          body(
              "Hop",
              i == SearchTemplateRoutes.MAX_ROUTE_HOPS ? "{ \"match_all\": {} }" : null,
              null);
      if (i < SearchTemplateRoutes.MAX_ROUTE_HOPS) {
        hop.setDefaultRoute(defaultTo("urn:li:searchTemplate:hop" + (i + 1)));
      }
      store(urn, hop);
      previous = urn;
    }
    templates
        .getDefaults()
        .put("fulltext", new HashMap<>(Map.of("default", "urn:li:searchTemplate:hop0")));
    assertThrows(
        SearchTemplateException.class,
        () -> selector.select(opContext, "customer orders", true, null));
    assertNotNull(previous);

    assertSame(SearchTemplateRoutes.pattern("^\\S+$"), SearchTemplateRoutes.pattern("^\\S+$"));
    SearchTemplateProperties shared = body("Shared", "{ \"match_all\": {} }", null);
    shared.setRoutes(routes(route("^\\S+$", SEARCH_TEMPLATE_EXACT_URN)));
    store("urn:li:searchTemplate:shared", shared);
    store(SEARCH_TEMPLATE_EXACT_URN, pinned("Exact", "{ \"match_all\": {} }"));
    MergedSearchTemplate first = loader.load(opContext, "urn:li:searchTemplate:shared");
    MergedSearchTemplate second = loader.load(opContext, "urn:li:searchTemplate:shared");
    assertSame(first, second);
    com.google.re2j.Pattern compiled = first.getRoutes().get(0).pattern();
    loader.invalidate("urn:li:searchTemplate:shared");
    MergedSearchTemplate reloaded = loader.load(opContext, "urn:li:searchTemplate:shared");
    assertNotSame(first, reloaded);
    assertSame(compiled, reloaded.getRoutes().get(0).pattern());
    assertSame(compiled, SearchTemplateRoutes.pattern("^\\S+$"));
  }

  @Test
  public void testScrollFollowsARouteAndExplainKeepsTheCacheKey() {
    installShippedRoutes();
    SearchTemplateCoordinator coordinator = new SearchTemplateCoordinator(v3Config(), null, loader);
    OperationContext scrolled = coordinator.bind(opContext, "*", true);
    assertEquals(
        scrolled.getSearchContext().getResolvedSearchTemplate().getUrn(), SEARCH_TEMPLATE_LIST_URN);

    OperationContext routed = coordinator.bind(opContext, "customer orders");
    OperationContext pinned =
        coordinator.bind(
            opContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSearchTemplate(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN))),
            "customer orders");
    assertEquals(
        routed.getSearchContext().getResolvedSearchTemplate().getRouteChain(),
        List.of(SEARCH_TEMPLATE_SEARCH_URN, SEARCH_TEMPLATE_FULLTEXT_URN));
    assertEquals(
        pinned.getSearchContext().getResolvedSearchTemplate().getRouteChain(),
        List.of(SEARCH_TEMPLATE_FULLTEXT_URN));
    assertEquals(
        routed.getSearchContext().getResolvedSearchTemplate().cacheKeyComponent(),
        pinned.getSearchContext().getResolvedSearchTemplate().cacheKeyComponent());

    OperationContext explained =
        opContext
            .withSearchFlags(flags -> flags.setFulltext(true).setIncludeExplain(true))
            .withResolvedSearchTemplate(routed.getSearchContext().getResolvedSearchTemplate());
    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits = mock(SearchHits.class);
    when(response.getHits()).thenReturn(hits);
    when(hits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));
    when(hits.getHits()).thenReturn(new SearchHit[0]);
    when(response.getAggregations()).thenReturn(null);
    when(response.getSuggest()).thenReturn(null);
    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            opContext,
            TestEntitySpecBuilder.getSpec(),
            v3Config(),
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    SearchResult withExplain = handler.extractResult(explained, response, null, 0, 10);
    assertEquals(
        withExplain.getMetadata().getRouteChain(),
        new com.linkedin.data.template.StringArray(
            List.of(SEARCH_TEMPLATE_SEARCH_URN, SEARCH_TEMPLATE_FULLTEXT_URN)));
    SearchResult withoutExplain = handler.extractResult(routed, response, null, 0, 10);
    assertFalse(withoutExplain.getMetadata().hasRouteChain());
  }

  @Test
  public void testRenderAndParentMerge() {
    SearchTemplateProperties fulltext = body("Fulltext", FULLTEXT_QUERY, null);
    fulltext.setFunctionScore(
        """
        { "functions": [], "score_mode": "multiply", "boost_mode": "multiply" }
        """);
    fulltext.setParameters(new StringMap(Map.of("deprecatedBoost", "0.25")));
    store(SEARCH_TEMPLATE_FULLTEXT_URN, fulltext);
    SearchTemplateProperties lineage = parentOf("Lineage", SEARCH_TEMPLATE_FULLTEXT_URN);
    lineage.setLastModified(new AuditStamp().setTime(20L).setActor(SYSTEM));
    fulltext.setLastModified(new AuditStamp().setTime(50L).setActor(SYSTEM));
    store(SEARCH_TEMPLATE_LINEAGE_URN, lineage);

    SearchTemplateRenderer renderer =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null);
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_LINEAGE_URN, 50L, "fulltext", null));
    String rendered =
        renderer.render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true).toString();
    assertTrue(rendered.contains("simple_query_string"));
    assertTrue(rendered.contains("orders"));
    assertTrue(rendered.contains("function_score"));
    assertTrue(rendered.contains("multiply"));
    assertTrue(bound.getSearchContext().getResolvedSearchTemplate().getHighlightFields().isEmpty());

    MergedSearchTemplate merged = loader.load(opContext, SEARCH_TEMPLATE_LINEAGE_URN);
    assertTrue(merged.getChain().contains(SEARCH_TEMPLATE_FULLTEXT_URN));
    assertEquals(merged.getLastModified(), 50L);
    fulltext.setQuery("{ \"term\": { \"name\": \"{{query}}\" } }");
    store(SEARCH_TEMPLATE_FULLTEXT_URN, fulltext);
    assertTrue(loader.load(opContext, SEARCH_TEMPLATE_LINEAGE_URN).getQuery().contains("bool"));
    loader.invalidate(SEARCH_TEMPLATE_FULLTEXT_URN);
    assertTrue(loader.load(opContext, SEARCH_TEMPLATE_LINEAGE_URN).getQuery().contains("term"));
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
  public void testValidator() {
    SearchConfiguration search = new SearchConfiguration();
    SearchV3Configuration v3 = new SearchV3Configuration();
    v3.setTemplates(templates);
    search.setV3(v3);
    SearchTemplateValidator validator =
        new SearchTemplateValidator(loader, search)
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
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(retriever)
            .build();

    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN),
                    USER,
                    ChangeType.UPSERT,
                    body("Fulltext", "{ \"match_all\": {} }", null)))
            .isEmpty());
    assertTrue(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN),
                    SYSTEM,
                    ChangeType.UPSERT,
                    body("Fulltext", "{ \"match_all\": {} }", null)))
            .isEmpty());
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN), SYSTEM, ChangeType.DELETE, null))
            .isEmpty());

    Urn custom = UrnUtils.getUrn("urn:li:searchTemplate:custom");
    SearchTemplateProperties locked = body("Locked", "{ \"match_all\": {} }", null);
    locked.setEditable(false);
    store(custom.toString(), locked);
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    custom, USER, ChangeType.UPSERT, body("Locked", "{ \"match_all\": {} }", null)))
            .isEmpty());

    SearchTemplateProperties child = body("Child", null, null);
    child.setParent(UrnUtils.getUrn("urn:li:searchTemplate:missing"));
    assertFalse(
        validate(validator, retrieverContext, item(custom, SYSTEM, ChangeType.UPSERT, child))
            .isEmpty());

    SearchTemplateProperties parent = body("Parent", "{ \"match_all\": {} }", null);
    parent.setDialect(SearchTemplateDialect.POSTGRES);
    store("urn:li:searchTemplate:pgParent", parent);
    SearchTemplateProperties mismatch = body("Mismatch", "{ \"match_all\": {} }", null);
    mismatch.setParent(UrnUtils.getUrn("urn:li:searchTemplate:pgParent"));
    assertFalse(
        validate(validator, retrieverContext, item(custom, SYSTEM, ChangeType.UPSERT, mismatch))
            .isEmpty());

    store(SEARCH_TEMPLATE_LIST_URN, body("List", "{ \"match_all\": {} }", LIST_SCORE));
    SearchTemplateProperties inheritedRescore = body("Inherited rescore", null, null);
    inheritedRescore.setParent(UrnUtils.getUrn(SEARCH_TEMPLATE_LIST_URN));
    inheritedRescore.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} } } }");
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(custom, SYSTEM, ChangeType.UPSERT, inheritedRescore))
            .isEmpty());

    Urn patched = UrnUtils.getUrn("urn:li:searchTemplate:patched");
    assertTrue(
        validate(validator, retrieverContext, item(patched, SYSTEM, ChangeType.PATCH, null))
            .isEmpty());
    ChangeMCP applied =
        change(
            patched,
            body("Patched", "{ \"script\": { \"script\": { \"source\": \"1\" } } }", null));
    assertFalse(
        validator
            .validatePreCommit(opContext, List.of(applied), retrieverContext)
            .collect(Collectors.toList())
            .isEmpty());
  }

  @Test
  public void testParentQueryKeepsHolesAnInheritingChildFills() {
    String parent = "urn:li:searchTemplate:custom";
    store(parent, body("Custom", HOLE_QUERY, null));
    SearchTemplateProperties child = parentOf("Child", parent);
    child.setQueryFragments(new StringMap(Map.of("extraDisMaxQueries", FILLED_HOLE)));
    store("urn:li:searchTemplate:child", child);
    SearchTemplateProperties grandchild = parentOf("Grandchild", "urn:li:searchTemplate:child");
    grandchild.setQueryFragments(new StringMap(Map.of("extraDisMaxQueries", FILLED_HOLE)));
    store("urn:li:searchTemplate:grandchild", grandchild);

    SearchTemplateValidator validator = templateValidator();
    RetrieverContext retrieverContext = holeContext();
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(parent),
                    SYSTEM,
                    ChangeType.UPSERT,
                    body("Custom", "{ \"match_all\": {} }", null)))
            .isEmpty());
    assertTrue(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn(parent),
                    SYSTEM,
                    ChangeType.UPSERT,
                    body("Custom", HOLE_QUERY, null)))
            .isEmpty());
  }

  @Test
  public void testChildThatSetsQueryStopsTheHoleWalk() {
    String parent = "urn:li:searchTemplate:custom";
    store(parent, body("Custom", HOLE_QUERY, null));
    SearchTemplateProperties child = body("Child", "{ \"match_all\": {} }", null);
    child.setParent(UrnUtils.getUrn(parent));
    store("urn:li:searchTemplate:child", child);
    SearchTemplateProperties grandchild = parentOf("Grandchild", "urn:li:searchTemplate:child");
    grandchild.setQueryFragments(new StringMap(Map.of("missingHole", FILLED_HOLE)));
    store("urn:li:searchTemplate:grandchild", grandchild);
    assertTrue(
        validate(
                templateValidator(),
                holeContext(),
                item(
                    UrnUtils.getUrn(parent),
                    SYSTEM,
                    ChangeType.UPSERT,
                    body("Custom", "{ \"match_all\": {} }", null)))
            .isEmpty());
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
    SearchTemplateCoordinator coordinator = new SearchTemplateCoordinator(elastic, null, loader);
    SearchTemplates.install(coordinator);
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
    assertNotNull(request.source().highlighter());
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
    rescored.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} }, \"query_weight\": 1.0, \"rescore_query_weight\": 1.0 } }");
    store(SEARCH_TEMPLATE_FULLTEXT_URN, rescored);
    loader.invalidate(SEARCH_TEMPLATE_FULLTEXT_URN);
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
    assertTrue(
        rescoredRequest.source().sorts() == null || rescoredRequest.source().sorts().isEmpty());

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
    assertFalse(rendered.contains("description.text"), rendered);
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
    SearchTemplates.install(new SearchTemplateCoordinator(v3Config(), null, loader));
    SearchConfiguration search = new SearchConfiguration();
    search.setPartial(new PartialConfiguration());
    search.setExactMatch(new ExactMatchConfiguration());
    search.setWordGram(new WordGramConfiguration());
    String rendered =
        new SearchQueryBuilder(search, null, false)
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
    SearchTemplateCoordinator coordinator = new SearchTemplateCoordinator(v3Config(), null, loader);
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
  public void testStampAndCacheInvalidation() {
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
    assertTrue(properties.hasLastModified());
    assertEquals(properties.getLastModified().getTime(), 5L);
    assertTrue(properties.hasCreated());

    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    assertTrue(
        loader.load(opContext, SEARCH_TEMPLATE_FULLTEXT_URN).getQuery().contains("match_all"));
    store(
        SEARCH_TEMPLATE_FULLTEXT_URN,
        body("Fulltext", "{ \"term\": { \"name\": \"next\" } }", null));
    assertTrue(
        loader.load(opContext, SEARCH_TEMPLATE_FULLTEXT_URN).getQuery().contains("match_all"));
    MCLItem mcl = mock(MCLItem.class);
    when(mcl.getUrn()).thenReturn(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN));
    when(mcl.getAspectName()).thenReturn(SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
    when(mcl.getChangeType()).thenReturn(ChangeType.UPSERT);
    MCPSideEffect sideEffect =
        new SearchTemplateCacheSideEffect(loader)
            .setConfig(
                pluginConfig(
                    SearchTemplateCacheSideEffect.class.getName(),
                    List.of("UPSERT", "UPDATE", "PATCH", "DELETE")));
    sideEffect.postApply(opContext, List.of(mcl), retrieverContext()).count();
    assertTrue(loader.load(opContext, SEARCH_TEMPLATE_FULLTEXT_URN).getQuery().contains("next"));
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
    assertFalse(
        keywordBound
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent()
            .endsWith("@knn"));
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
    loader.invalidate(SEARCH_TEMPLATE_FULLTEXT_URN);
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
    loader.invalidate(SEARCH_TEMPLATE_FULLTEXT_URN);
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
  public void testParentIsSearchableAndStaysOutOfTheDefaultQuery() {
    var fields = registry.getEntitySpec(SEARCH_TEMPLATE_ENTITY_NAME).getSearchableFieldSpecs();
    assertEquals(fields.size(), 1);
    assertEquals(fields.get(0).getSearchableAnnotation().getFieldName(), "parent");
    assertFalse(fields.get(0).getSearchableAnnotation().isQueryByDefault());
  }

  @Test
  public void testSelectIsAConjunctionAndStopIsADisjunction() {
    SearchTemplateLadder select = new SearchTemplateLadder();
    select.setSelect(new StringArray("scoreSort", "pageSize", "notColumnFilter"));
    assertTrue(SearchTemplateLadderPredicates.selectMatches(select, "orders", null, null, 10));
    assertFalse(
        SearchTemplateLadderPredicates.selectMatches(
            select,
            "orders",
            null,
            List.of(new SortCriterion().setField("name").setOrder(SortOrder.ASCENDING)),
            10));
    assertFalse(SearchTemplateLadderPredicates.selectMatches(select, "orders", null, null, 0));

    SearchTemplateLadder stop = new SearchTemplateLadder();
    SearchTemplateLadderStop minHits = new SearchTemplateLadderStop();
    minHits.setMinHits(1);
    SearchTemplateLadderStop shape = new SearchTemplateLadderStop();
    shape.setEmptyWhen("spacelessIdentifier");
    stop.setStop(new SearchTemplateLadderStopArray(minHits, shape));
    assertTrue(SearchTemplateLadderPredicates.stopMatches(stop, 0, "a.b.c.d"));
    assertFalse(SearchTemplateLadderPredicates.stopMatches(stop, 0, "orders"));
    assertTrue(SearchTemplateLadderPredicates.stopMatches(stop, 2, "orders"));
  }

  @Test
  public void testLastRungEmptyResultIsKeptAndExplicitUrnFollowsNext() {
    SearchTemplateProperties entry = body("Entry", "{ \"match_all\": {} }", null);
    entry.setLadder(stopLadder(1, "urn:li:searchTemplate:broad"));
    store("urn:li:searchTemplate:entry", entry);
    store("urn:li:searchTemplate:broad", body("Broad", "{ \"match_all\": {} }", null));

    Set<String> called = ConcurrentHashMap.newKeySet();
    SearchTemplateWalk.Served<SearchResult> served =
        walk(
            "urn:li:searchTemplate:entry",
            "orders",
            null,
            10,
            urn -> {
              called.add(urn);
              return page(
                  urn.endsWith("broad") ? 4 : 0,
                  "urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)");
            });
    assertEquals(served.urn(), "urn:li:searchTemplate:broad");
    assertEquals(called, Set.of("urn:li:searchTemplate:entry", "urn:li:searchTemplate:broad"));

    SearchTemplateProperties last = body("Last", "{ \"match_all\": {} }", null);
    last.setLadder(stopLadder(1, null));
    store("urn:li:searchTemplate:last", last);
    SearchTemplateWalk.Served<SearchResult> empty =
        walk("urn:li:searchTemplate:last", "orders", null, 10, urn -> page(0));
    assertEquals(empty.page().getNumEntities(), 0);
    assertEquals(empty.urn(), "urn:li:searchTemplate:last");

    assertThrows(
        RuntimeException.class,
        () ->
            walk(
                "urn:li:searchTemplate:last",
                "orders",
                null,
                10,
                urn -> {
                  throw new IllegalStateException("shard");
                }));
  }

  @Test
  public void testParallelAnyCancelsAndAllEmptyPrefersTheFirstListedStop() throws Exception {
    String slow = "urn:li:searchTemplate:slow";
    String fast = "urn:li:searchTemplate:fast";
    SearchTemplateProperties slowBody = body("Slow", "{ \"match_all\": {} }", null);
    slowBody.setLadder(stopLadder(1, null));
    SearchTemplateProperties fastBody = body("Fast", "{ \"match_all\": {} }", null);
    fastBody.setLadder(stopLadder(1, null));
    store(slow, slowBody);
    store(fast, fastBody);
    SearchTemplateProperties any = body("Any", "{ \"match_all\": {} }", null);
    SearchTemplateLadder anyLadder = new SearchTemplateLadder();
    anyLadder.setParallel(new UrnArray(UrnUtils.getUrn(slow), UrnUtils.getUrn(fast)));
    anyLadder.setJoin("any");
    any.setLadder(anyLadder);
    store("urn:li:searchTemplate:any", any);

    CountDownLatch slowEntered = new CountDownLatch(1);
    AtomicBoolean slowFinished = new AtomicBoolean();
    long started = System.nanoTime();
    SearchTemplateWalk.Served<SearchResult> served =
        walk(
            "urn:li:searchTemplate:any",
            "orders",
            null,
            10,
            urn -> {
              if (urn.equals(slow)) {
                slowEntered.countDown();
                try {
                  Thread.sleep(30_000);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return page(0);
                }
                slowFinished.set(true);
                return page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,slow,PROD)");
              }
              try {
                assertTrue(slowEntered.await(2, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return page(0);
              }
              return page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,fast,PROD)");
            });
    assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5));
    assertEquals(served.urn(), fast);
    assertFalse(slowFinished.get());

    String first = "urn:li:searchTemplate:firstListed";
    String second = "urn:li:searchTemplate:secondListed";
    SearchTemplateProperties firstBody = body("First", "{ \"match_all\": {} }", null);
    firstBody.setLadder(stopLadder(1, null));
    SearchTemplateProperties secondBody = body("Second", "{ \"match_all\": {} }", null);
    secondBody.setLadder(stopLadder(1, null));
    store(first, firstBody);
    store(second, secondBody);
    SearchTemplateProperties allEmpty = body("All empty", "{ \"match_all\": {} }", null);
    SearchTemplateLadder allLadder = new SearchTemplateLadder();
    allLadder.setParallel(new UrnArray(UrnUtils.getUrn(first), UrnUtils.getUrn(second)));
    allLadder.setJoin("allEmpty");
    allEmpty.setLadder(allLadder);
    store("urn:li:searchTemplate:allEmpty", allEmpty);
    SearchTemplateWalk.Served<SearchResult> waited =
        walk(
            "urn:li:searchTemplate:allEmpty",
            "orders",
            null,
            10,
            urn -> {
              if (urn.equals(first)) {
                try {
                  Thread.sleep(150);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return page(0);
                }
              }
              String name = urn.equals(first) ? "first" : "second";
              return page(1, "urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)");
            });
    assertEquals(waited.urn(), first);

    SearchTemplateProperties merge = body("Merge", "{ \"match_all\": {} }", null);
    SearchTemplateLadder mergeLadder = new SearchTemplateLadder();
    mergeLadder.setParallel(new UrnArray(UrnUtils.getUrn(first), UrnUtils.getUrn(second)));
    mergeLadder.setJoin("merge");
    mergeLadder.setNext(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN));
    merge.setLadder(mergeLadder);
    store("urn:li:searchTemplate:merge", merge);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    assertThrows(
        SearchTemplateException.class,
        () ->
            walk(
                "urn:li:searchTemplate:merge",
                "orders",
                null,
                10,
                urn -> page(1, "urn:li:dataset:(urn:li:dataPlatform:hive,member,PROD)")));
  }

  @Test
  public void testParallelOverflowRunsOnCaller() {
    String first = "urn:li:searchTemplate:poolFirst";
    String second = "urn:li:searchTemplate:poolSecond";
    store(first, body("First", "{ \"match_all\": {} }", null));
    store(second, body("Second", "{ \"match_all\": {} }", null));
    SearchTemplateProperties group = body("Group", "{ \"match_all\": {} }", null);
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setParallel(new UrnArray(UrnUtils.getUrn(first), UrnUtils.getUrn(second)));
    ladder.setJoin("allEmpty");
    group.setLadder(ladder);
    store("urn:li:searchTemplate:poolGroup", group);

    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            1,
            1,
            60,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    String caller = Thread.currentThread().getName();
    Set<String> threads = ConcurrentHashMap.newKeySet();
    try {
      walk(
          executor,
          "urn:li:searchTemplate:poolGroup",
          "orders",
          null,
          10,
          urn -> {
            threads.add(Thread.currentThread().getName());
            return page(0);
          });
    } finally {
      executor.shutdownNow();
    }
    assertTrue(threads.contains(caller));
  }

  @Test
  public void testViewerPlaceholderAndQueryByDefaultFields() {
    SearchTemplateProperties viewed =
        body("Viewed", "{ \"term\": { \"urn\": \"{{viewerUrn}}\" } }", null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, viewed);
    ElasticSearchConfiguration elastic = v3Config();
    SearchTemplateCoordinator coordinator = new SearchTemplateCoordinator(elastic, null, loader);
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
    loader.invalidate(SEARCH_TEMPLATE_FULLTEXT_URN);
    OperationContext plainBound = coordinator.bind(opContext, "customer orders");
    assertFalse(
        plainBound
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent()
            .contains("@viewer="));
    List<String> expected =
        SearchTemplateFields.analyzedFields(
            new V3SearchQueryBuilder(new SearchConfiguration(), null)
                .searchedFieldConfigs(opContext, List.of(TestEntitySpecBuilder.getSpec())));
    String fields =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
            .render(plainBound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    for (String field : expected) {
      assertTrue(fields.contains(field), field);
    }
    assertTrue(fields.contains("textFieldOverride.text"), fields);
  }

  @Test
  public void testScrollOmitsRescoreAndRerankStaysOnSearch() throws Exception {
    SearchTemplateProperties rescored = body("Fulltext", "{ \"match_all\": {} }", null);
    rescored.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} }, \"query_weight\": 1.0, \"rescore_query_weight\": 1.0 } }");
    store(SEARCH_TEMPLATE_FULLTEXT_URN, rescored);
    ElasticSearchConfiguration elastic = v3Config();
    SearchTemplates.install(new SearchTemplateCoordinator(elastic, null, loader));
    OperationContext searching =
        opContext
            .withSearchFlags(flags -> flags.setFulltext(true))
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    SearchRequest request =
        SearchRequestHandler.getBuilder(
                searching,
                TestEntitySpecBuilder.getSpec(),
                elastic,
                null,
                QueryFilterRewriteChain.EMPTY,
                TEST_SEARCH_SERVICE_CONFIG)
            .getSearchRequest(
                searching, "customer orders", null, null, null, null, null, 10, List.of());
    assertTrue(request.source().rescores() == null || request.source().rescores().isEmpty());
    assertTrue(
        request.source().sorts().stream().anyMatch(sort -> sort instanceof ScoreSortBuilder));
    assertTrue(
        request.source().sorts().stream()
            .anyMatch(
                sort ->
                    sort instanceof FieldSortBuilder field && "urn".equals(field.getFieldName())));

    Path source =
        Path.of(
            "src/main/java/com/linkedin/metadata/search/client/CachingEntitySearchService.java");
    if (!Files.exists(source)) {
      source = Path.of("metadata-io").resolve(source);
    }
    String text = Files.readString(source);
    int scroll = text.indexOf("private ScrollResult getRawScrollResults");
    int next = text.indexOf("private boolean enableCache", scroll);
    assertTrue(scroll > 0 && next > scroll);
    assertFalse(text.substring(scroll, next).contains("rerank"));
  }

  @Test
  public void testLadderRequiresStopAndRejectsDialectCycles() {
    SearchTemplateProperties missingStop = body("Missing", "{ \"match_all\": {} }", null);
    SearchTemplateLadder nextOnly = new SearchTemplateLadder();
    nextOnly.setNext(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN));
    missingStop.setLadder(nextOnly);
    assertThrows(
        SearchTemplateException.class, () -> SearchTemplateChecks.check(missingStop, 1024));

    SearchTemplateProperties parent = body("Parent", "{ \"match_all\": {} }", null);
    parent.setLadder(stopLadder(1, SEARCH_TEMPLATE_FULLTEXT_URN));
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    store("urn:li:searchTemplate:parentLadder", parent);
    SearchTemplateProperties child = parentOf("Child", "urn:li:searchTemplate:parentLadder");
    child.setLadder(new SearchTemplateLadder());
    store("urn:li:searchTemplate:childLadder", child);
    assertNull(loader.load(opContext, "urn:li:searchTemplate:childLadder").getLadder());
    assertNotNull(loader.load(opContext, "urn:li:searchTemplate:parentLadder").getLadder());

    SearchConfiguration search = new SearchConfiguration();
    SearchV3Configuration v3 = new SearchV3Configuration();
    v3.setTemplates(templates);
    search.setV3(v3);
    SearchTemplateValidator validator =
        new SearchTemplateValidator(loader, search)
            .setConfig(
                AspectPluginConfig.builder()
                    .className(SearchTemplateValidator.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName(SEARCH_TEMPLATE_ENTITY_NAME)
                                .aspectName("*")
                                .build()))
                    .build());
    RetrieverContext retrieverContext = retrieverContext();

    SearchTemplateProperties postgres = body("Pg", "select 1", null);
    postgres.setDialect(SearchTemplateDialect.POSTGRES);
    store("urn:li:searchTemplate:pg", postgres);
    SearchTemplateProperties mismatched = body("Es", "{ \"match_all\": {} }", null);
    SearchTemplateLadder toPg = stopLadder(1, "urn:li:searchTemplate:pg");
    mismatched.setLadder(toPg);
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:mismatched"),
                    SYSTEM,
                    ChangeType.UPSERT,
                    mismatched))
            .isEmpty());

    SearchTemplateProperties left = body("Left", "{ \"match_all\": {} }", null);
    left.setLadder(stopLadder(1, "urn:li:searchTemplate:right"));
    SearchTemplateProperties right = body("Right", "{ \"match_all\": {} }", null);
    right.setLadder(stopLadder(1, "urn:li:searchTemplate:left"));
    store("urn:li:searchTemplate:left", left);
    store("urn:li:searchTemplate:right", right);
    assertFalse(
        validate(
                validator,
                retrieverContext,
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:left"), SYSTEM, ChangeType.UPSERT, left))
            .isEmpty());
  }

  private SearchTemplateWalk.Served<SearchResult> walk(
      String urn,
      String query,
      List<SortCriterion> sort,
      Integer size,
      Function<String, SearchResult> pages) {
    return walk(ForkJoinPool.commonPool(), urn, query, sort, size, pages);
  }

  private SearchTemplateWalk.Served<SearchResult> walk(
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
        new SearchTemplateWalk.Request(query, null, sort, size),
        templateUrn -> {
          try {
            SearchResult page = pages.apply(templateUrn);
            return SearchTemplateWalk.AttemptResult.ok(
                page, page.getNumEntities(), new ArrayList<>(page.getEntities()), List.of(), false);
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

  private static SearchResult page(int total, String... urns) {
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

  private static SearchTemplateLadder parallelLadder(String join, String next, boolean withStop) {
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

  private static SearchTemplateLadder stopLadder(int minHits, String next) {
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    SearchTemplateLadderStop stop = new SearchTemplateLadderStop();
    stop.setMinHits(minHits);
    ladder.setStop(new SearchTemplateLadderStopArray(stop));
    if (next != null) {
      ladder.setNext(UrnUtils.getUrn(next));
    }
    return ladder;
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
    SearchTemplateCoordinator coordinator = new SearchTemplateCoordinator(elastic, null, loader);
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
                        "domains", List.of("urn:li:domain:finance"), 1.5)))
            .toString();
    assertTrue(weighted.contains("1.5"));
    assertTrue(weighted.contains("urn:li:domain:finance"));
    assertTrue(weighted.contains("terms"));

    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", FULLTEXT_QUERY, null));
    loader.invalidate(SEARCH_TEMPLATE_FULLTEXT_URN);
    OperationContext plain = coordinator.bind(opContext, "customer orders");
    assertFalse(
        plain
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent()
            .contains("@domains="));
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

  private ElasticSearchConfiguration v3Config() {
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

  private void store(String urn, SearchTemplateProperties properties) {
    if (!properties.hasLastModified()) {
      properties.setLastModified(new AuditStamp().setTime(1L).setActor(SYSTEM));
    }
    stored.put(UrnUtils.getUrn(urn), properties);
  }

  private static DataHubPolicyInfo domainPolicy(
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

  @Test
  public void testStructuredPropertyUrnConstantAndKeyword() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    definitions.put(property, definition("certification.status", "string", null));
    SearchTemplateProperties template =
        body(
            "Property",
            "{ \"term\": { \"{{urn:li:structuredProperty:certification.status}}.keyword\": \"gold\" } }",
            null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
    OperationContext bound =
        new SearchTemplateCoordinator(v3Config(), null, loader).bind(opContext, "customer orders");
    String rendered =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    assertTrue(rendered.contains("structuredProperties.certification_status.keyword"));
    assertFalse(rendered.contains("urn:li:structuredProperty:"));
  }

  @Test
  public void testStructuredPropertyVersionChangesTheField() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    definitions.put(property, definition("certification.status", "string", "00000000000001"));
    SearchTemplateProperties template =
        body(
            "Versioned",
            "{ \"term\": { \"{{urn:li:structuredProperty:certification.status}}\": \"gold\" } }",
            null);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
    OperationContext bound =
        new SearchTemplateCoordinator(v3Config(), null, loader).bind(opContext, "customer orders");
    String rendered =
        new SearchTemplateRenderer(loader, new SearchConfiguration(), null)
            .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "orders", true)
            .toString();
    assertTrue(rendered.contains("structuredProperties._versioned.certification_status."));
    assertTrue(rendered.contains("00000000000001"));
  }

  @Test
  public void testUnknownStructuredPropertyAndIllegalKeywordAreRejected() {
    SearchTemplateProperties unknown =
        body(
            "Missing",
            "{ \"term\": { \"{{urn:li:structuredProperty:missing}}\": \"gold\" } }",
            null);
    assertThrows(
        SearchTemplateException.class,
        () -> SearchTemplateChecks.check(unknown, 1024, opContext, retriever, null));

    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:priority");
    definitions.put(property, definition("priority", "number", null));
    SearchTemplateProperties numbered = body("Number", "{ \"match_all\": {} }", null);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    SearchFieldBoost boost = new SearchFieldBoost();
    boost.setFieldName(property.toString());
    boost.setSubfield(StructuredPropertyReferences.KEYWORD);
    boost.setBoost(2.0);
    fields.setAdd(new SearchFieldBoostArray(boost));
    numbered.setFields(fields);
    assertThrows(
        SearchTemplateException.class,
        () -> SearchTemplateChecks.check(numbered, 1024, opContext, retriever, null));
  }

  @Test
  public void testStructuredPropertyQueryFieldUsesTheExistingPath() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    StructuredPropertyDefinition defined = definition("certification.status", "string", null);
    DataHubSearchConfig config = new DataHubSearchConfig();
    config.setQueryByDefault(true);
    config.setBoostScore(3.0);
    defined.setSearchConfiguration(config);
    SearchFieldConfig field = StructuredPropertyQueryFields.queryField(property, defined);
    assertEquals(field.fieldName(), "structuredProperties.certification_status");
    assertEquals(field.boost(), 3.0f);
    assertFalse(field.fieldName().contains("customProperties"));
    assertThrows(
        SearchTemplateException.class,
        () ->
            StructuredPropertyReferences.resolveMember(
                "structuredProperties._versioned.certification_status.00000000000001.string",
                null,
                null,
                null));
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
  public void testScrollLeavesAnUnreadNextUnchecked() {
    String entry = "urn:li:searchTemplate:scrollEntry";
    SearchTemplateProperties properties = body("Scroll", "{ \"match_all\": {} }", null);
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setNext(UrnUtils.getUrn("urn:li:searchTemplate:missing-next"));
    properties.setLadder(ladder);
    store(entry, properties);
    templates.getDefaults().put("fulltext", Map.of("default", entry));
    SearchTemplateCoordinator coordinator = new SearchTemplateCoordinator(v3Config(), null, loader);
    assertThrows(
        SearchTemplateException.class, () -> coordinator.bind(opContext, "customer orders"));
    OperationContext scrolled = coordinator.bind(opContext, "customer orders", true);
    assertEquals(scrolled.getSearchContext().getResolvedSearchTemplate().getUrn(), entry);
    assertFalse(
        scrolled
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent()
            .contains("missing-next"));
  }

  @Test
  public void testStructuredPropertyLogicalNameResolvesToTheIndexedPath() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    definitions.put(property, definition("certification.status", "string", null));
    assertEquals(
        StructuredPropertyReferences.resolveMember(
            "structuredProperties.certification.status", null, opContext, retriever),
        "structuredProperties.certification_status");

    definitions.put(property, definition("certification.status", "string", "00000000000001"));
    String versioned =
        StructuredPropertyReferences.resolveMember(
            "structuredProperties.certification.status", null, opContext, retriever);
    assertTrue(versioned.startsWith("structuredProperties._versioned."));
    assertTrue(versioned.contains("certification_status"));
  }

  @Test
  public void testWriteTimePlaceholdersUseRuntimeFieldPaths() {
    Map<String, Object> context = SearchTemplateMustache.validationContext("");
    assertEquals(context.get("nameKeyword"), SearchTemplateFields.keywordPath("name"));
    assertEquals(context.get("nameText"), SearchTemplateFields.textPath("name"));
    assertEquals(context.get("columnNameField"), SearchTemplateFields.keywordPath("columns"));
    assertEquals(context.get("entityNameField"), SearchTemplateFields.keywordPath("name"));
    assertEquals(context.get("descriptionField"), SearchTemplateFields.textPath("description"));
    assertFalse(String.valueOf(context.get("qualifiedNameKeyword")).contains("_search."));
    assertFalse(String.valueOf(context.get("idKeyword")).contains("_search."));
  }

  @Test
  public void testOverrideAcceptsAQueryByDefaultStructuredProperty() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    StructuredPropertyDefinition defined = definition("certification.status", "string", null);
    DataHubSearchConfig config = new DataHubSearchConfig();
    config.setQueryByDefault(true);
    config.setBoostScore(3.0);
    defined.setSearchConfiguration(config);
    definitions.put(property, defined);
    SearchTemplateProperties template = body("Override", "{ \"match_all\": {} }", null);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    SearchFieldBoost override = new SearchFieldBoost();
    override.setFieldName("structuredProperties.certification.status");
    override.setBoost(4.0);
    fields.setOverride(new SearchFieldBoostArray(override));
    template.setFields(fields);
    assertThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateChecks.check(
                template, 1024, opContext, retriever, List.of(TestEntitySpecBuilder.getSpec())));
    StructuredPropertyQueryFields.replace(List.of(Pair.of(property, defined)));
    try {
      SearchTemplateChecks.check(
          template, 1024, opContext, retriever, List.of(TestEntitySpecBuilder.getSpec()));
    } finally {
      StructuredPropertyQueryFields.replace(List.of());
    }
  }

  @Test
  public void testHighlightNameIsNotExpanded() {
    SearchTemplateProperties template = body("Highlights", "{ \"match_all\": {} }", null);
    HighlightFieldOverrides highlights = new HighlightFieldOverrides();
    highlights.setAdd(new StringArray("tags_1"));
    template.setHighlightFields(highlights);
    store("urn:li:searchTemplate:literalHighlight", template);
    ElasticSearchConfiguration elastic = v3Config();
    SearchTemplates.install(new SearchTemplateCoordinator(elastic, null, loader));
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(
                "urn:li:searchTemplate:literalHighlight", 1L, "fulltext", null));
    SearchRequest request = searchRequest(bound, elastic, "orders");
    assertNotNull(request.source().highlighter());
    List<String> names =
        request.source().highlighter().fields().stream().map(field -> field.name()).toList();
    assertEquals(names, List.of("tags_1"));

    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    OperationContext empty =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    SearchRequest blank = searchRequest(empty, elastic, "orders");
    assertNotNull(blank.source().highlighter());
    assertTrue(blank.source().highlighter().fields().isEmpty());
  }

  @Test
  public void testQueryByDefaultStringPropertyIsATerm() {
    Urn stringProperty = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn numberProperty = UrnUtils.getUrn("urn:li:structuredProperty:priority");
    Urn urnProperty = UrnUtils.getUrn("urn:li:structuredProperty:owner");
    StructuredPropertyDefinition stringDefinition =
        definition("certification.status", "string", null);
    DataHubSearchConfig stringConfig = new DataHubSearchConfig();
    stringConfig.setQueryByDefault(true);
    stringConfig.setBoostScore(3.0);
    stringDefinition.setSearchConfiguration(stringConfig);
    StructuredPropertyDefinition numberDefinition = definition("priority", "number", null);
    DataHubSearchConfig numberConfig = new DataHubSearchConfig();
    numberConfig.setQueryByDefault(true);
    numberDefinition.setSearchConfiguration(numberConfig);
    StructuredPropertyDefinition urnDefinition = definition("owner", "urn", null);
    DataHubSearchConfig urnConfig = new DataHubSearchConfig();
    urnConfig.setQueryByDefault(true);
    urnDefinition.setSearchConfiguration(urnConfig);
    StructuredPropertyQueryFields.replace(
        List.of(
            Pair.of(stringProperty, stringDefinition),
            Pair.of(numberProperty, numberDefinition),
            Pair.of(urnProperty, urnDefinition)));
    try {
      store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", FULLTEXT_QUERY, null));
      OperationContext bound =
          opContext.withResolvedSearchTemplate(
              new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
      SearchTemplateRenderer renderer =
          new SearchTemplateRenderer(loader, new SearchConfiguration(), null);
      String rendered =
          renderer.render(bound, List.of(TestEntitySpecBuilder.getSpec()), "gold", true).toString();
      assertTrue(rendered.contains("structuredProperties.certification_status.keyword"), rendered);
      assertTrue(rendered.contains("gold"), rendered);
      assertFalse(rendered.contains("structuredProperties.certification_status^"), rendered);
      assertFalse(rendered.contains("structuredProperties.priority"), rendered);
      assertTrue(rendered.contains("\"structuredProperties.owner\""), rendered);
      assertFalse(rendered.contains("structuredProperties.owner.keyword"), rendered);

      String phrase =
          renderer
              .render(bound, List.of(TestEntitySpecBuilder.getSpec()), "gold certified", true)
              .toString();
      assertTrue(phrase.contains("gold certified"), phrase);
    } finally {
      StructuredPropertyQueryFields.replace(List.of());
    }
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
    store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
    ElasticSearchConfiguration elastic = v3Config();
    SearchTemplates.install(new SearchTemplateCoordinator(elastic, null, loader));
    OperationContext searching =
        opContext
            .withSearchFlags(
                flags -> flags.setFulltext(true).setSkipHighlighting(true).setSkipAggregates(true))
            .withResolvedSearchTemplate(
                new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            searching,
            TestEntitySpecBuilder.getSpec(),
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
    SearchTemplateCoordinator coordinator = new SearchTemplateCoordinator(elastic, null, loader);
    assertThrows(
        SearchTemplateException.class,
        () -> coordinator.bind(opContext, "customer orders", false, List.of("dataset")));
    OperationContext scrolled =
        coordinator.bind(opContext, "customer orders", true, List.of("dataset"));
    assertEquals(
        scrolled.getSearchContext().getResolvedSearchTemplate().getUrn(),
        "urn:li:searchTemplate:knnChild");
  }

  @Test
  public void testStructuredPropertyQueryInheritsUntilCleared() {
    String parentUrn = "urn:li:searchTemplate:parentNameValue";
    SearchTemplateProperties parent = body("Parent", "{ \"match_all\": {} }", null);
    parent.setStructuredPropertyQuery(nameAndValue(8f));
    store(parentUrn, parent);

    SearchTemplateProperties child = parentOf("Child", parentUrn);
    MergedSearchTemplate inherited =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childNameValue", child);
    assertEquals(inherited.getStructuredPropertyQuery().getNameAndValueWeight(), 8f);

    child.setStructuredPropertyQuery(nameAndValue(4f));
    MergedSearchTemplate replaced =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childNameValue", child);
    assertEquals(replaced.getStructuredPropertyQuery().getNameAndValueWeight(), 4f);

    child.setStructuredPropertyQuery(new StructuredPropertyQuery());
    MergedSearchTemplate dropped =
        loader.preview(opContext, retriever, "urn:li:searchTemplate:childNameValue", child);
    assertNull(dropped.getStructuredPropertyQuery());
  }

  @Test
  public void testNameAndValueWeightMustBePositive() {
    SearchTemplateProperties empty = body("Empty", "{ \"match_all\": {} }", null);
    empty.setStructuredPropertyQuery(new StructuredPropertyQuery());
    SearchTemplateChecks.check(empty, 1024);

    SearchTemplateProperties positive = body("Positive", "{ \"match_all\": {} }", null);
    positive.setStructuredPropertyQuery(nameAndValue(8f));
    SearchTemplateChecks.check(positive, 1024);

    SearchTemplateProperties zero = body("Zero", "{ \"match_all\": {} }", null);
    zero.setStructuredPropertyQuery(nameAndValue(0f));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(zero, 1024));

    SearchTemplateProperties negative = body("Negative", "{ \"match_all\": {} }", null);
    negative.setStructuredPropertyQuery(nameAndValue(-1f));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(negative, 1024));
  }

  @Test
  public void testNameAndValueExamples() {
    installCertification(null);
    try {
      store(SEARCH_TEMPLATE_FULLTEXT_URN, nameValueTemplate(null));
      String gold = renderStored("gold", false);
      assertTrue(gold.contains("structuredProperties.certification_status.keyword"), gold);
      assertTrue(gold.contains("gold"), gold);
      assertFalse(gold.contains("dis_max"), gold);

      String named = renderStored("certification status gold", false);
      assertTrue(named.contains("dis_max"), named);
      assertTrue(named.contains("\"gold\""), named);
      assertTrue(named.contains("structuredProperties.certification_status.keyword"), named);
      assertTrue(named.contains("certification status gold"), named);

      String silver = renderStored("certification status silver", false);
      assertFalse(silver.contains("gold"), silver);
      assertTrue(silver.contains("\"silver\""), silver);

      String nameOnly = renderStored("certification status", false);
      assertFalse(nameOnly.contains("dis_max"), nameOnly);

      String trailing = renderStored("status gold", false);
      assertFalse(trailing.contains("dis_max"), trailing);

      String qualified = renderStored("certification.status gold", false);
      assertTrue(qualified.contains("dis_max"), qualified);
      assertTrue(qualified.contains("\"gold\""), qualified);
    } finally {
      StructuredPropertyQueryFields.replace(List.of());
    }
  }

  @Test
  public void testRemovingAPropertyDropsBothClauses() {
    Urn property = installCertification(null);
    try {
      SearchTemplateProperties template = nameValueTemplate(null);
      SearchFieldOverrides fields = new SearchFieldOverrides();
      fields.setRemove(new StringArray(property.toString()));
      template.setFields(fields);
      store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
      String named = renderStored("certification status gold", false);
      String value = renderStored("gold", false);
      assertFalse(named.contains("structuredProperties.certification_status"), named);
      assertFalse(named.contains("dis_max"), named);
      assertFalse(value.contains("structuredProperties.certification_status"), value);
    } finally {
      StructuredPropertyQueryFields.replace(List.of());
    }
  }

  @Test
  public void testOverrideDoesNotChangeNameAndValueWeight() {
    Urn property = installCertification(null);
    try {
      SearchTemplateProperties template = nameValueTemplate(null);
      SearchFieldOverrides fields = new SearchFieldOverrides();
      SearchFieldBoost override = new SearchFieldBoost();
      override.setFieldName(property.toString());
      override.setBoost(4.0);
      fields.setOverride(new SearchFieldBoostArray(override));
      template.setFields(fields);
      store(SEARCH_TEMPLATE_FULLTEXT_URN, template);
      String named = renderStored("certification status gold", false);
      assertTrue(named.contains("\"boost\" : 8.0"), named);
      assertTrue(named.contains("\"boost\" : 4.0"), named);
    } finally {
      StructuredPropertyQueryFields.replace(List.of());
    }
  }

  @Test
  public void testNameAndValueSkipsDateNumberAndUrn() {
    Urn notes = UrnUtils.getUrn("urn:li:structuredProperty:notes");
    Urn priority = UrnUtils.getUrn("urn:li:structuredProperty:priority");
    Urn due = UrnUtils.getUrn("urn:li:structuredProperty:due");
    Urn owner = UrnUtils.getUrn("urn:li:structuredProperty:owner");
    StructuredPropertyDefinition notesDefinition =
        searchableProperty("notes", "rich_text", "Notes");
    StructuredPropertyDefinition priorityDefinition =
        searchableProperty("priority", "number", "Priority");
    StructuredPropertyDefinition dueDefinition = searchableProperty("due", "date", "Due Date");
    StructuredPropertyDefinition ownerDefinition = searchableProperty("owner", "urn", "Owner");
    definitions.put(notes, notesDefinition);
    definitions.put(priority, priorityDefinition);
    definitions.put(due, dueDefinition);
    definitions.put(owner, ownerDefinition);
    StructuredPropertyQueryFields.replace(
        List.of(
            Pair.of(notes, notesDefinition),
            Pair.of(priority, priorityDefinition),
            Pair.of(due, dueDefinition),
            Pair.of(owner, ownerDefinition)));
    try {
      store(SEARCH_TEMPLATE_FULLTEXT_URN, nameValueTemplate(null));
      String rich = renderStored("notes gold", false);
      assertTrue(rich.contains("dis_max"), rich);
      assertTrue(rich.contains("structuredProperties.notes.keyword"), rich);
      assertTrue(rich.contains("\"gold\""), rich);

      String numbered = renderStored("priority gold", false);
      assertFalse(numbered.contains("dis_max"), numbered);
      assertFalse(numbered.contains("structuredProperties.priority"), numbered);

      String dated = renderStored("due date gold", false);
      assertFalse(dated.contains("dis_max"), dated);
      assertFalse(dated.contains("structuredProperties.due"), dated);

      String urned = renderStored("owner gold", false);
      assertFalse(urned.contains("dis_max"), urned);
      assertFalse(urned.contains("\"gold\""), urned);
    } finally {
      StructuredPropertyQueryFields.replace(List.of());
    }
  }

  @Test
  public void testNameAndValueDisMaxSitsInsideFunctionScore() {
    installCertification(null);
    try {
      store(
          SEARCH_TEMPLATE_FULLTEXT_URN,
          nameValueTemplate(
              "{ \"functions\": [], \"score_mode\": \"multiply\", \"boost_mode\": \"multiply\" }"));
      String rendered = renderStored("certification status gold", false);
      int functionScore = rendered.indexOf("\"function_score\"");
      int disMax = rendered.indexOf("\"dis_max\"");
      assertTrue(functionScore >= 0, rendered);
      assertTrue(disMax > functionScore, rendered);
      assertTrue(rendered.contains("\"tie_breaker\" : 0.0"), rendered);
    } finally {
      StructuredPropertyQueryFields.replace(List.of());
    }
  }

  @Test
  public void testNameAndValueStaysOnScroll() {
    installCertification(null);
    try {
      store(SEARCH_TEMPLATE_FULLTEXT_URN, nameValueTemplate(null));
      String rendered = renderStored("certification status gold", true);
      assertTrue(rendered.contains("dis_max"), rendered);
      assertTrue(rendered.contains("\"gold\""), rendered);
    } finally {
      StructuredPropertyQueryFields.replace(List.of());
    }
  }

  @Test
  public void testCatalogStampChangesWhenDisplayNameChanges() {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    StructuredPropertyDefinition defined =
        searchableProperty("certification.status", "string", "Certification Status");
    definitions.put(property, defined);
    StructuredPropertyQueryFields.replace(List.of(Pair.of(property, defined)));
    try {
      store(SEARCH_TEMPLATE_FULLTEXT_URN, nameValueTemplate(null));
      SearchTemplateCoordinator coordinator =
          new SearchTemplateCoordinator(v3Config(), null, loader);
      String first =
          coordinator
              .bind(opContext, "customer orders")
              .getSearchContext()
              .getResolvedSearchTemplate()
              .cacheKeyComponent();
      assertTrue(first.contains("|sp="), first);

      defined.setDisplayName("Certification Level");
      StructuredPropertyQueryFields.replace(List.of(Pair.of(property, defined)));
      String renamed =
          coordinator
              .bind(opContext, "customer orders")
              .getSearchContext()
              .getResolvedSearchTemplate()
              .cacheKeyComponent();
      assertNotEquals(first, renamed);

      store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
      loader.invalidate(SEARCH_TEMPLATE_FULLTEXT_URN);
      String without =
          coordinator
              .bind(opContext, "customer orders")
              .getSearchContext()
              .getResolvedSearchTemplate()
              .cacheKeyComponent();
      defined.setDisplayName("Certification Status");
      StructuredPropertyQueryFields.replace(List.of(Pair.of(property, defined)));
      String still =
          coordinator
              .bind(opContext, "customer orders")
              .getSearchContext()
              .getResolvedSearchTemplate()
              .cacheKeyComponent();
      assertEquals(without, still);
      assertFalse(without.contains("|sp="), without);
    } finally {
      StructuredPropertyQueryFields.replace(List.of());
    }
  }

  private Urn installCertification(String version) {
    Urn property = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    StructuredPropertyDefinition defined =
        searchableProperty("certification.status", "string", "Certification Status");
    if (version != null) {
      defined.setVersion(version);
    }
    definitions.put(property, defined);
    StructuredPropertyQueryFields.replace(List.of(Pair.of(property, defined)));
    return property;
  }

  private static StructuredPropertyDefinition searchableProperty(
      String qualifiedName, String valueType, String displayName) {
    StructuredPropertyDefinition defined = definition(qualifiedName, valueType, null);
    defined.setDisplayName(displayName);
    DataHubSearchConfig config = new DataHubSearchConfig();
    config.setQueryByDefault(true);
    config.setBoostScore(3.0);
    defined.setSearchConfiguration(config);
    return defined;
  }

  private static SearchTemplateProperties nameValueTemplate(String functionScore) {
    SearchTemplateProperties template = body("Fulltext", "{ \"match_all\": {} }", functionScore);
    template.setStructuredPropertyQuery(nameAndValue(8f));
    return template;
  }

  private static StructuredPropertyQuery nameAndValue(float weight) {
    StructuredPropertyQuery section = new StructuredPropertyQuery();
    section.setNameAndValueWeight(weight);
    return section;
  }

  private String renderStored(String query, boolean scroll) {
    OperationContext bound =
        opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    return new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
        .render(bound, List.of(TestEntitySpecBuilder.getSpec()), query, true, null, scroll)
        .query()
        .toString();
  }

  private SearchRequest searchRequest(
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

  private static SearchResponse parseSearchResponse(String json) throws Exception {
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

  private static StructuredPropertyDefinition definition(
      String qualifiedName, String valueType, String version) {
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setQualifiedName(qualifiedName);
    definition.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub." + valueType));
    definition.setEntityTypes(
        new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.testEntity")));
    if (version != null) {
      definition.setVersion(version);
    }
    return definition;
  }

  private static final String ROUTE_QUERY =
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

  @Test
  public void testFilterCountAndAggregateRenderOneTemplate() {
    installShippedRoutes();
    store(
        SEARCH_TEMPLATE_LIST_URN,
        pinned("List", "{ \"term\": { \"templateMarker\": \"list-body\" } }"));
    store(
        "urn:li:searchTemplate:customList",
        pinned("Custom", "{ \"term\": { \"templateMarker\": \"custom-body\" } }"));
    ElasticSearchConfiguration elastic = v3Config();
    SearchTemplates.install(new SearchTemplateCoordinator(elastic, null, loader));
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
    String aggregateQuery =
        dao.buildAggregateByValue(opContext, List.of(DATASET_ENTITY_NAME), "platform", null, 10)
            .source()
            .query()
            .toString();
    assertTrue(filterQuery.contains("list-body"), filterQuery);
    assertTrue(countQuery.contains("list-body"), countQuery);
    assertTrue(aggregateQuery.contains("list-body"), aggregateQuery);
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
                opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH_ACROSS_LINEAGE),
                "*",
                true)
            .toString();
    String keywordQuery =
        handler
            .getSearchRequest(opContext, "customer orders", null, null, 0, 10, List.of())
            .source()
            .query()
            .toString();
    assertTrue(searchQuery.contains("list-body"), searchQuery);
    assertTrue(scrollQuery.contains("list-body"), scrollQuery);
    assertTrue(browseQuery.contains("list-body"), browseQuery);
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

    SearchTemplates.install(null);
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

  private void installShippedRoutes() {
    SearchTemplateProperties search = body("Search", null, null);
    search.setRoutes(searchRoutes());
    search.setDefaultRoute(defaultTo(SEARCH_TEMPLATE_FULLTEXT_URN));
    store(SEARCH_TEMPLATE_SEARCH_URN, search);
    store(SEARCH_TEMPLATE_FULLTEXT_URN, pinned("Fulltext", ROUTE_QUERY));
    store(SEARCH_TEMPLATE_LIST_URN, pinned("List", "{ \"match_all\": {} }"));
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
    SearchTemplateProperties lineage = parentOf("Lineage", SEARCH_TEMPLATE_FULLTEXT_URN);
    lineage.setRoutes(searchRoutes());
    store(SEARCH_TEMPLATE_LINEAGE_URN, lineage);
    SearchTemplateProperties postgres = body("Postgres", "to_tsquery('simple', '{{query}}')", null);
    postgres.setDialect(SearchTemplateDialect.POSTGRES);
    store("urn:li:searchTemplate:fulltextPostgres", postgres);
    templates
        .getDefaults()
        .put("fulltext", new HashMap<>(Map.of("default", SEARCH_TEMPLATE_SEARCH_URN)));
    templates
        .getSurfaces()
        .put(
            SearchTemplateSurfaces.SEARCH_ACROSS_LINEAGE,
            Map.of("fulltext", Map.of("default", SEARCH_TEMPLATE_LINEAGE_URN)));
  }

  private static SearchTemplateRouteArray searchRoutes() {
    return routes(
        route("(?i)^(?:urn:.*|(?:s3|gs|hdfs)://.*)", SEARCH_TEMPLATE_IDENTITY_URN),
        route("^\\*?$", SEARCH_TEMPLATE_LIST_URN),
        route("^\\\\/q [\"'].+[\"']$", SEARCH_TEMPLATE_STRUCTURED_EXACT_URN),
        route("^\\\\/q .*", SEARCH_TEMPLATE_STRUCTURED_URN),
        route("^[\"'].+[\"']$", SEARCH_TEMPLATE_EXACT_URN),
        route("^\\S+$", SEARCH_TEMPLATE_EXACT_URN));
  }

  private static SearchTemplateRouteArray structuredRoutes() {
    return routes(
        route("(?i)^(?:urn:.*|(?:s3|gs|hdfs)://.*)", SEARCH_TEMPLATE_IDENTITY_URN),
        route("^\\*?$", SEARCH_TEMPLATE_LIST_URN),
        route("^(?:\\\\/q )?[\"'].+[\"']$", SEARCH_TEMPLATE_STRUCTURED_EXACT_URN));
  }

  private static SearchTemplateProperties pinned(String name, String query) {
    SearchTemplateProperties properties = body(name, query, null);
    properties.setRoutes(new SearchTemplateRouteArray());
    return properties;
  }

  private static SearchTemplateProperties bodyWithRoute(String regex, String target) {
    SearchTemplateProperties properties = body("Routed", "{ \"match_all\": {} }", null);
    properties.setRoutes(routes(route(regex, target)));
    return properties;
  }

  private static SearchTemplateRoute route(String regex, String urn) {
    SearchTemplateRoute entry = new SearchTemplateRoute();
    entry.setQueryRegex(regex);
    entry.setTemplate(UrnUtils.getUrn(urn));
    return entry;
  }

  private static SearchTemplateRouteArray routes(SearchTemplateRoute... entries) {
    return new SearchTemplateRouteArray(List.of(entries));
  }

  private static SearchTemplateDefaultRoute defaultTo(String urn) {
    SearchTemplateDefaultRoute route = new SearchTemplateDefaultRoute();
    route.setTemplate(UrnUtils.getUrn(urn));
    return route;
  }

  private static SearchTemplateProperties body(String name, String query, String functionScore) {
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

  private static SearchTemplateProperties parentOf(String name, String parent) {
    SearchTemplateProperties properties = body(name, null, null);
    properties.setParent(UrnUtils.getUrn(parent));
    return properties;
  }

  private List<AspectValidationException> validate(
      SearchTemplateValidator validator, RetrieverContext retrieverContext, MCPItem item) {
    return validator
        .validateProposed(opContext, List.of(item), retrieverContext, null)
        .collect(Collectors.toList());
  }

  private SearchTemplateValidator templateValidator() {
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

  private RetrieverContext holeContext() {
    return RetrieverContext.builder()
        .graphRetriever(GraphRetriever.EMPTY)
        .searchRetriever(childrenByParent())
        .cachingAspectRetriever(retriever)
        .build();
  }

  private SearchRetriever childrenByParent() {
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

  private static String parentFilter(Filter filters) {
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

  private RetrieverContext retrieverContext() {
    return RetrieverContext.builder()
        .graphRetriever(GraphRetriever.EMPTY)
        .searchRetriever(SearchRetriever.EMPTY)
        .cachingAspectRetriever(retriever)
        .build();
  }

  private static AspectPluginConfig pluginConfig(String className, List<String> operations) {
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

  private static ChangeMCP change(Urn urn, SearchTemplateProperties properties) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getAspectName()).thenReturn(SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getAuditStamp()).thenReturn(new AuditStamp().setTime(5L).setActor(SYSTEM));
    when(item.getRecordTemplate()).thenReturn(properties);
    when(item.getMetadataChangeProposal()).thenReturn(null);
    return item;
  }

  private static MCPItem item(
      Urn urn, Urn actor, ChangeType changeType, SearchTemplateProperties properties) {
    MCPItem item = mock(MCPItem.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getAspectName()).thenReturn(SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
    when(item.getChangeType()).thenReturn(changeType);
    when(item.getAuditStamp()).thenReturn(new AuditStamp().setTime(5L).setActor(actor));
    when(item.getRecordTemplate()).thenReturn(properties);
    when(item.getMetadataChangeProposal()).thenReturn(null);
    return item;
  }
}
