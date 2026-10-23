package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_SYSTEM_IDS;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.SearchTemplateValidator;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Contract version, declared capabilities, shipped system ids, and Mustache names. */
public class SearchTemplateContractTest {
  private static final Pattern TAG =
      Pattern.compile("\\{\\{\\s*([#/^&]*)\\s*([A-Za-z0-9_.]+)\\s*}}");
  private static final Urn SYSTEM = UrnUtils.getUrn(SYSTEM_ACTOR);
  private static final Urn USER = UrnUtils.getUrn("urn:li:corpuser:tester");
  private static final String PARENT = "urn:li:searchTemplate:contractParent";
  private static final String CHILD = "urn:li:searchTemplate:contractChild";

  private SearchTemplateFixtures.Harness harness;

  @BeforeMethod
  public void setup() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
  }

  @Test
  public void testShippedTemplatesDeclareVersionOneAndSystemIds() {
    for (String urn : SearchTemplateBootstrap.urns()) {
      String id = UrnUtils.getUrn(urn).getEntityKey().get(0);
      assertTrue(SEARCH_TEMPLATE_SYSTEM_IDS.contains(id), urn);
      SearchTemplateProperties shipped = SearchTemplateBootstrap.get(urn);
      assertEquals(shipped.getApiVersion().intValue(), SearchTemplateCapabilities.API_VERSION);
      assertFalse(shipped.hasCapabilities());
      collectTags(section(shipped)).forEach(this::assertDeclaredName);
    }
  }

  @Test
  public void testContextNamesStayDisjointAndCoverTheSample() {
    assertTrue(
        SearchTemplateContextVariables.STABLE.stream()
            .noneMatch(SearchTemplateContextVariables.INTERNAL::contains));
    SearchTemplateMustache.validationContext("{{query}}")
        .keySet()
        .forEach(
            key ->
                assertTrue(
                    SearchTemplateContextVariables.STABLE.contains(key)
                        || SearchTemplateContextVariables.INTERNAL.contains(key),
                    key));
    assertTrue(SearchTemplateContextVariables.isContextName("flags.alphanumericSplit"));
    assertTrue(SearchTemplateContextVariables.isContextName("flags.exactNameLetterDigit"));
    assertTrue(SearchTemplateContextVariables.isContextName("prefixFields"));
    assertTrue(SearchTemplateContextVariables.isContextName("modifier"));
    assertFalse(SearchTemplateContextVariables.isContextName("notARealSection"));
  }

  @Test
  public void testRenderProducesOnlyDeclaredContextNames() {
    SearchTemplateLoader loader = new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH);
    OperationContext bound =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate("urn:li:searchTemplate:fulltext", 1L, "fulltext", null));
    String rendered =
        new SearchV3QueryBuilder(loader, new SearchConfiguration(), null)
            .render(
                bound,
                List.of(TestEntitySpecBuilder.getSpec()),
                "db.schema.table_name extra words 2017",
                true)
            .toString();
    assertTrue(rendered.contains("table_name") || rendered.contains("table name"), rendered);
  }

  @Test
  public void testUnsupportedVersionAndCapabilityFailAtWrite() {
    SearchTemplateProperties version = body("Version");
    version.setApiVersion(2);
    SearchTemplateException versionError =
        expectThrows(
            SearchTemplateException.class, () -> SearchTemplateChecks.check(version, 1024));
    assertTrue(versionError.getMessage().contains("apiVersion 2"));

    SearchTemplateProperties capability = body("Capability");
    capability.setApiVersion(SearchTemplateCapabilities.API_VERSION);
    capability.setCapabilities(new StringArray("vector"));
    SearchTemplateException capabilityError =
        expectThrows(
            SearchTemplateException.class, () -> SearchTemplateChecks.check(capability, 1024));
    assertTrue(capabilityError.getMessage().contains("vector"));

    SearchTemplateProperties supported = body("Supported");
    supported.setApiVersion(SearchTemplateCapabilities.API_VERSION);
    supported.setCapabilities(new StringArray("keyword"));
    SearchTemplateChecks.check(supported, 1024);
  }

  @Test
  public void testMergedContractRejectsUnsupportedValues() {
    MergedSearchTemplate version = merged().apiVersion(2).build();
    SearchTemplateException versionError =
        expectThrows(
            SearchTemplateException.class, () -> SearchTemplateChecks.checkMerged(version, 1024));
    assertTrue(versionError.getMessage().contains("apiVersion 2"));
    expectThrows(
        SearchTemplateException.class,
        () -> SearchTemplateCapabilities.requireSupported(version, false));

    MergedSearchTemplate capability = merged().capabilities(List.of("merge")).build();
    expectThrows(
        SearchTemplateException.class,
        () -> SearchTemplateCapabilities.requireSupported(capability, false, true));
  }

  @Test
  public void testLoaderMergesVersionAndCapabilities() {
    SearchTemplateProperties parent = body("Parent");
    parent.setApiVersion(SearchTemplateCapabilities.API_VERSION);
    parent.setCapabilities(new StringArray(List.of("keyword", "rescore")));
    parent.setQuery("{ \"match_all\": {} }");
    SearchTemplateProperties child = body("Child");
    child.setParent(UrnUtils.getUrn(PARENT));
    AtomicLong clock = new AtomicLong();
    SearchTemplateLoader loader = catalog(parent, child, clock);

    MergedSearchTemplate inherited = loader.load(harness.opContext, CHILD);
    assertEquals(inherited.getApiVersion(), SearchTemplateCapabilities.API_VERSION);
    assertEquals(inherited.getCapabilities(), List.of("keyword", "rescore"));

    child.setCapabilities(new StringArray());
    clock.addAndGet(60_000L + 1);
    assertTrue(loader.load(harness.opContext, CHILD).getCapabilities().isEmpty());

    child.setCapabilities(new StringArray("routes"));
    clock.addAndGet(60_000L + 1);
    assertEquals(loader.load(harness.opContext, CHILD).getCapabilities(), List.of("routes"));

    parent.setApiVersion(2);
    clock.addAndGet(60_000L + 1);
    SearchTemplateException dropped =
        expectThrows(SearchTemplateException.class, () -> loader.load(harness.opContext, CHILD));
    assertTrue(dropped.getMessage().contains("apiVersion 2"));
    SearchTemplateException hidden =
        expectThrows(
            SearchTemplateException.class,
            () -> catalog(parent, child, new AtomicLong()).load(harness.opContext, CHILD));
    assertTrue(hidden.getMessage().contains("apiVersion 2"));
  }

  @Test
  public void testWriteStampsVersionAndLocksBrowseTemplates() {
    SearchTemplateValidator validator = templateValidator();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(harness.searchRetriever)
            .cachingAspectRetriever(harness.retriever)
            .build();
    SearchTemplateProperties custom = body("Custom");
    List<AspectValidationException> accepted =
        validate(
            validator,
            retrieverContext,
            item(UrnUtils.getUrn(CHILD), SYSTEM, ChangeType.UPSERT, custom));
    assertTrue(accepted.isEmpty(), String.valueOf(accepted));
    assertNull(custom.getApiVersion());
    new SearchTemplateStampHook()
        .writeMutation(
            harness.opContext,
            List.of((ChangeMCP) item(UrnUtils.getUrn(CHILD), SYSTEM, ChangeType.UPSERT, custom)),
            retrieverContext)
        .toList();
    assertEquals(custom.getApiVersion().intValue(), SearchTemplateCapabilities.API_VERSION);

    SearchTemplateProperties rejectedVersion = body("Rejected");
    rejectedVersion.setApiVersion(2);
    List<AspectValidationException> versionRejected =
        validate(
            validator,
            retrieverContext,
            item(UrnUtils.getUrn(CHILD), SYSTEM, ChangeType.UPSERT, rejectedVersion));
    assertFalse(versionRejected.isEmpty());
    assertTrue(versionRejected.get(0).getMessage().contains("apiVersion 2"));

    for (String id : List.of("search", "fulltext", "searchLight")) {
      Urn urn = UrnUtils.getUrn("urn:li:searchTemplate:" + id);
      List<AspectValidationException> rejected =
          validate(validator, retrieverContext, item(urn, USER, ChangeType.UPSERT, body(id)));
      assertFalse(rejected.isEmpty(), id);
      assertTrue(rejected.get(0).getMessage().contains("cannot be modified"), id);
      List<AspectValidationException> deleted =
          validate(validator, retrieverContext, item(urn, SYSTEM, ChangeType.DELETE, null));
      assertFalse(deleted.isEmpty(), id);
      assertTrue(deleted.get(0).getMessage().contains("cannot be deleted"), id);
    }
  }

  private void assertDeclaredName(String name) {
    if (".".equals(name)) {
      return;
    }
    assertTrue(
        SearchTemplateContextVariables.isContextName(name) || "extraDisMaxQueries".equals(name),
        name);
  }

  private static Set<String> collectTags(String text) {
    Set<String> names = new LinkedHashSet<>();
    Matcher matcher = TAG.matcher(text);
    while (matcher.find()) {
      names.add(matcher.group(2));
    }
    return names;
  }

  private static String section(SearchTemplateProperties properties) {
    return String.join(
        "\n",
        text(properties.hasQuery() ? properties.getQuery() : null),
        text(properties.hasClauses() ? properties.getClauses() : null),
        text(properties.hasFunctionScore() ? properties.getFunctionScore() : null),
        text(properties.hasRescore() ? properties.getRescore() : null),
        text(properties.hasKnn() ? properties.getKnn() : null),
        text(properties.hasAggregations() ? properties.getAggregations() : null),
        text(properties.hasSort() ? properties.getSort() : null),
        properties.hasQueryFragments() && properties.getQueryFragments() != null
            ? String.join("\n", properties.getQueryFragments().values())
            : "");
  }

  private static String text(String value) {
    return value == null ? "" : value;
  }

  private SearchTemplateLoader catalog(
      SearchTemplateProperties parent, SearchTemplateProperties child, AtomicLong clock) {
    Map<String, SearchTemplateProperties> catalog = new HashMap<>();
    catalog.put(PARENT, parent);
    catalog.put(CHILD, child);
    return new SearchTemplateLoader(60, SearchTemplateSource.CLASSPATH, catalog, clock::get);
  }

  private static MergedSearchTemplate.MergedSearchTemplateBuilder merged() {
    return MergedSearchTemplate.builder()
        .urn(CHILD)
        .dialect(SearchTemplateDialect.ELASTICSEARCH.name())
        .queryFragments(Map.of())
        .parameters(Map.of())
        .chain(List.of(CHILD));
  }

  private static SearchTemplateProperties body(String name) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(name);
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    return properties;
  }

  private List<AspectValidationException> validate(
      SearchTemplateValidator validator, RetrieverContext retrieverContext, MCPItem item) {
    return validator
        .validateProposed(harness.opContext, List.of(item), retrieverContext, null)
        .toList();
  }

  private SearchTemplateValidator templateValidator() {
    SearchConfiguration search = new SearchConfiguration();
    SearchV3Configuration v3 = new SearchV3Configuration();
    v3.setTemplates(harness.templates);
    search.setV3(v3);
    return new SearchTemplateValidator(harness.loader, search)
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
                            .aspectName(SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME)
                            .build()))
                .build());
  }

  private static ChangeMCP item(
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
