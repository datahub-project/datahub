package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_EXACT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_IDENTITY_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LIGHT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LIST_BY_URN_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_LIST_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_SEARCH_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_STRUCTURED_EXACT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_STRUCTURED_URN;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.re2j.Pattern;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.aspect.validation.SearchTemplateValidator;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRouteArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.exception.ActorAccessException;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.Test;

public class SearchTemplateRouteTest extends SearchTemplateFixture {
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
    assertEquals(
        selector.select(opContext, "s3:\\/\\/bucket\\/key", true, null).getUrn(),
        SEARCH_TEMPLATE_IDENTITY_URN);
    assertEquals(
        selector.select(opContext, "", true, null).getUrn(), SEARCH_TEMPLATE_LIST_BY_URN_URN);
    assertEquals(
        selector.select(opContext, "*", true, null).getUrn(), SEARCH_TEMPLATE_LIST_BY_URN_URN);
    assertEquals(
        selector.select(opContext, "\\/q \"orders\"", true, null).getUrn(),
        SEARCH_TEMPLATE_STRUCTURED_EXACT_URN);
    assertEquals(
        selector.select(opContext, "\\/q platform:snowflake", true, null).getUrn(),
        SEARCH_TEMPLATE_STRUCTURED_URN);
    assertEquals(
        selector.select(opContext, "\\/q *", true, null).getUrn(), SEARCH_TEMPLATE_LIST_URN);
    assertEquals(
        selector
            .select(opContext, "\\/q urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)", true, null)
            .getUrn(),
        SEARCH_TEMPLATE_IDENTITY_URN);
    assertNotEquals(
        selector.select(opContext, "urn:orders", true, null).getUrn(),
        SEARCH_TEMPLATE_IDENTITY_URN);
    assertNotEquals(
        selector.select(opContext, "URN:orders", true, null).getUrn(),
        SEARCH_TEMPLATE_IDENTITY_URN);
    assertNotEquals(
        selector.select(opContext, "urn:", true, null).getUrn(), SEARCH_TEMPLATE_IDENTITY_URN);
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
    assertEquals(
        selector.select(opContext, "\\/q *", false, null).getUrn(), SEARCH_TEMPLATE_LIST_URN);
  }

  @Test
  public void testShippedLightAndIdentityRecall() {
    SearchTemplateLoader classpath = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    MergedSearchTemplate searchLight =
        classpath.load(opContext, "urn:li:searchTemplate:searchLight");
    assertEquals(SearchTemplateRoutes.decide(searchLight, "*").target(), SEARCH_TEMPLATE_LIST_URN);
    assertEquals(
        SearchTemplateRoutes.decide(searchLight, "urn:orders").target(), SEARCH_TEMPLATE_LIGHT_URN);
    assertEquals(
        SearchTemplateRoutes.decide(searchLight, "URN:orders").target(), SEARCH_TEMPLATE_LIGHT_URN);
    assertEquals(
        SearchTemplateRoutes.decide(searchLight, "urn:").target(), SEARCH_TEMPLATE_LIGHT_URN);
    assertEquals(
        SearchTemplateRoutes.decide(searchLight, "urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)")
            .target(),
        SEARCH_TEMPLATE_IDENTITY_URN);

    MergedSearchTemplate light = classpath.load(opContext, SEARCH_TEMPLATE_LIGHT_URN);
    assertTrue(
        light.getLadder().getStop().stream()
            .anyMatch(stop -> "spacelessIdentifier".equals(stop.getEmptyWhen())));
    assertTrue(SearchTemplateQueryShape.stopOnEmptyLight("1234567"));
    assertTrue(SearchTemplateQueryShape.stopOnEmptyLight("a.b.c.d"));
    assertTrue(SearchTemplateQueryShape.stopOnEmptyLight("a:b:c:d"));
    assertFalse(SearchTemplateQueryShape.stopOnEmptyLight("a__b_c"));
    assertTrue(
        SearchTemplateLadderPredicates.stopMatches(light.getLadder(), 0, "123456", null, null, 10));
    assertTrue(
        SearchTemplateLadderPredicates.stopMatches(
            light.getLadder(), 0, "a.b.c.d", null, null, 10));
    assertFalse(
        SearchTemplateLadderPredicates.stopMatches(light.getLadder(), 0, "a__b_c", null, null, 10));
    assertFalse(
        SearchTemplateLadderPredicates.stopMatches(light.getLadder(), 0, "orders", null, null, 10));
    assertTrue(
        SearchTemplateLadderPredicates.selectMatches(light.getLadder(), "orders", null, null, 10));
    assertTrue(
        SearchTemplateLadderPredicates.selectMatches(
            light.getLadder(), "customer orders", null, null, 10));
    assertFalse(
        SearchTemplateLadderPredicates.selectMatches(
            light.getLadder(), "one two three four", null, null, 10));

    SearchV3QueryBuilder builder =
        new SearchV3QueryBuilder(classpath, new SearchConfiguration(), null);
    assertTrue(
        render(builder, SEARCH_TEMPLATE_LIGHT_URN, "sales orders2017").contains("multi_match"));
    assertFalse(render(builder, SEARCH_TEMPLATE_LIGHT_URN, "orders2017").contains("multi_match"));
    assertFalse(
        render(builder, SEARCH_TEMPLATE_FULLTEXT_URN, "orderz_20240101").contains("orderz~"));
    assertTrue(render(builder, SEARCH_TEMPLATE_FULLTEXT_URN, "custmers").contains("custmers~"));
  }

  @Test
  public void testLongQueryScoresAPartialDescriptionMatch() {
    SearchTemplateLoader classpath = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    SearchV3QueryBuilder builder =
        new SearchV3QueryBuilder(classpath, new SearchConfiguration(), null);
    String longQuery = "one two three four";
    String fulltext = render(builder, SEARCH_TEMPLATE_FULLTEXT_URN, longQuery);
    assertTrue(fulltext.contains("\"minimum_should_match\" : \"25%\""), fulltext);
    assertTrue(fulltext.contains("\"operator\" : \"OR\""), fulltext);
    assertTrue(fulltext.contains("\"boost\" : 10.0"), fulltext);

    // The exact name term is boost 100 on every query. The partial description clause is the
    // minimum_should_match and boost 10, and those stay off a short query.
    String shortQuery = render(builder, SEARCH_TEMPLATE_FULLTEXT_URN, "orders");
    assertFalse(shortQuery.contains("\"minimum_should_match\" : \"25%\""), shortQuery);
    assertFalse(shortQuery.contains("\"boost\" : 10.0"), shortQuery);

    String light = render(builder, SEARCH_TEMPLATE_LIGHT_URN, longQuery);
    assertTrue(light.contains("\"boost\" : 100.0"), light);
    assertFalse(light.contains("\"minimum_should_match\" : \"25%\""), light);
  }

  private String render(SearchV3QueryBuilder builder, String template, String query) {
    return builder
        .render(
            opContext.withResolvedSearchTemplate(
                new ResolvedSearchTemplate(template, 1L, null, null)),
            List.of(TestEntitySpecBuilder.getSpec()),
            query,
            true)
        .toString();
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
  public void testCallerTemplateRequiresSystemOperationsPrivilege() {
    installShippedRoutes();
    SearchTemplateSelector selector = new SearchTemplateSelector(templates, loader);
    OperationContext denied =
        opContext
            .asSession(RequestContext.TEST, Authorizer.EMPTY, TestOperationContexts.TEST_USER_AUTH)
            .withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSearchTemplate(UrnUtils.getUrn(SEARCH_TEMPLATE_LIST_URN)));

    assertThrows(ActorAccessException.class, () -> selector.select(denied, "orders", true, null));
  }

  @Test
  public void testEscapedStoragePathSelectsIdentity() {
    SearchTemplateLoader classpath = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    for (String urn : List.of(SEARCH_TEMPLATE_SEARCH_URN, SEARCH_TEMPLATE_STRUCTURED_URN)) {
      MergedSearchTemplate template = classpath.load(opContext, urn);
      for (String scheme : List.of("s3", "gs", "hdfs")) {
        assertEquals(
            SearchTemplateRoutes.decide(template, scheme + "://bucket/key").target(),
            SEARCH_TEMPLATE_IDENTITY_URN,
            urn);
        assertEquals(
            SearchTemplateRoutes.decide(template, scheme + ":\\/\\/bucket\\/key").target(),
            SEARCH_TEMPLATE_IDENTITY_URN,
            urn);
      }
      assertFalse(
          SEARCH_TEMPLATE_IDENTITY_URN.equals(
              SearchTemplateRoutes.decide(template, "orders").target()),
          urn);
    }
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

    SearchTemplateProperties withComponent = body("Router", null, null);
    com.linkedin.search.template.SearchTemplateComponent component =
        new com.linkedin.search.template.SearchTemplateComponent();
    component.setName("reverse");
    component.setVersion(1);
    component.setMode(com.linkedin.search.template.SearchTemplateComponentMode.POST_PROCESS);
    withComponent.setComponent(component);
    withComponent.setDefaultRoute(defaultTo(SEARCH_TEMPLATE_FULLTEXT_URN));
    assertThrows(
        SearchTemplateException.class, () -> SearchTemplateChecks.check(withComponent, 1024));

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
                    "a".repeat(templates.getMaxRoutePatternLength() + 1), SEARCH_TEMPLATE_LIST_URN),
                1024,
                templates.getMaxRoutes(),
                templates.getMaxRoutePatternLength(),
                null,
                null,
                null));

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
        validate(
                validator,
                empty,
                item(
                    UrnUtils.getUrn(SEARCH_TEMPLATE_SEARCH_URN), SYSTEM, ChangeType.UPSERT, absent))
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
    for (int i = 0; i <= templates.getMaxRouteHops(); i++) {
      String urn = "urn:li:searchTemplate:hop" + i;
      SearchTemplateProperties hop =
          body("Hop", i == templates.getMaxRouteHops() ? "{ \"match_all\": {} }" : null, null);
      if (i < templates.getMaxRouteHops()) {
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

    assertSame(
        SearchTemplateRoutes.pattern("^\\S+$", templates.getMaxRoutePatternLength()),
        SearchTemplateRoutes.pattern("^\\S+$", templates.getMaxRoutePatternLength()));
    SearchTemplateProperties shared = body("Shared", "{ \"match_all\": {} }", null);
    shared.setRoutes(routes(route("^\\S+$", SEARCH_TEMPLATE_EXACT_URN)));
    store("urn:li:searchTemplate:shared", shared);
    store(SEARCH_TEMPLATE_EXACT_URN, pinned("Exact", "{ \"match_all\": {} }"));
    MergedSearchTemplate first = loader.load(opContext, "urn:li:searchTemplate:shared");
    MergedSearchTemplate second = loader.load(opContext, "urn:li:searchTemplate:shared");
    assertSame(first, second);
    Pattern compiled = first.getRoutes().get(0).pattern();
    expire();
    MergedSearchTemplate reloaded = loader.load(opContext, "urn:li:searchTemplate:shared");
    assertNotSame(first, reloaded);
    assertSame(compiled, reloaded.getRoutes().get(0).pattern());
    assertSame(
        compiled, SearchTemplateRoutes.pattern("^\\S+$", templates.getMaxRoutePatternLength()));
  }

  @Test
  public void testScrollFollowsARouteAndExplainKeepsTheCacheKey() {
    installShippedRoutes();
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            v3Config(),
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    OperationContext scrolled = coordinator.bind(opContext, "*", true);
    assertEquals(
        scrolled.getSearchContext().getResolvedSearchTemplate().getUrn(),
        SEARCH_TEMPLATE_LIST_BY_URN_URN);

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
        new StringArray(List.of(SEARCH_TEMPLATE_SEARCH_URN, SEARCH_TEMPLATE_FULLTEXT_URN)));
    SearchResult withoutExplain = handler.extractResult(routed, response, null, 0, 10);
    assertFalse(withoutExplain.getMetadata().hasRouteChain());
  }

  @Test
  public void testParentIsSearchableAndStaysOutOfTheDefaultQuery() {
    var fields = registry.getEntitySpec(SEARCH_TEMPLATE_ENTITY_NAME).getSearchableFieldSpecs();
    assertEqualsNoOrder(
        fields.stream().map(field -> field.getSearchableAnnotation().getFieldName()).toArray(),
        new String[] {"dialect", "apiVersion", "parent", "editable"});
    assertTrue(
        fields.stream().noneMatch(field -> field.getSearchableAnnotation().isQueryByDefault()));
  }
}
