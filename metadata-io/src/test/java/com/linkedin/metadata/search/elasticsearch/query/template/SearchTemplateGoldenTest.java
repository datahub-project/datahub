package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsUtils;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ForkJoinPool;
import org.opensearch.index.query.QueryBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Renders the shipped templates against the real entity registry and the committed property
 * catalog. A schema or search-option change fails here so the author can see the search impact.
 */
public class SearchTemplateGoldenTest {
  private static final String IMPACT =
      "This is the search impact of a schema or search-option change. "
          + "Change the @Searchable annotation or the structured-property definition if the field "
          + "should stay out of default search. Set UPDATE_SEARCH_TEMPLATE_GOLDEN=1 to rewrite "
          + "the output golden after that review. Input fixtures are not rewritten.";

  private SearchTemplateFixtures.Harness harness;

  @BeforeMethod
  public void setup() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    harness.installCatalog();
    harness.installShippedTemplates();
  }

  @Test
  public void testRenderedQueriesAndFieldManifest() throws Exception {
    EntitySpec dataset = harness.registry.getEntitySpec("dataset");
    String fulltext =
        render(SEARCH_TEMPLATE_FULLTEXT_URN, SearchTemplateFixtures.FULLTEXT_QUERY, dataset);
    String structured =
        render("urn:li:searchTemplate:structured", SearchTemplateFixtures.FULLTEXT_QUERY, dataset);
    String fieldEdits =
        render(SearchTemplateFixtures.FIELD_EDITS, SearchTemplateFixtures.FULLTEXT_QUERY, dataset);
    String nameAndValue =
        render(
            SearchTemplateFixtures.NAME_AND_VALUE,
            SearchTemplateFixtures.NAME_VALUE_QUERY,
            dataset);
    assertDatasetFulltext(fulltext);
    assertFieldEdits(fieldEdits);
    assertNameAndValue(nameAndValue);
    assertTrue(datasetIndexRoots().contains("urn"));
    assertTrue(
        sharedFields().containsAll(List.of("name", "qualifiedName", "description", "other")));
    ObjectNode fieldManifest = manifest();
    ArrayNode queried = fieldManifest.putArray("templateFieldRoots");
    Set<String> roots = new TreeSet<>();
    for (String query : List.of(fulltext, structured, fieldEdits, nameAndValue)) {
      Set<String> fields = new LinkedHashSet<>();
      collectReferencedFields(SearchTemplateFixtures.JSON.readTree(query), fields);
      fields.forEach(field -> roots.add(fieldRoot(field)));
    }
    roots.forEach(queried::add);
    compare("dataset-fulltext.json", canonicalize(fulltext));
    compare("dataset-structured.json", canonicalize(structured));
    compare("dataset-field-edits.json", canonicalize(fieldEdits));
    compare("dataset-name-and-value.json", canonicalize(nameAndValue));
    compare("field-manifest.json", canonicalize(fieldManifest.toString()));
  }

  @Test
  public void testFulltextPrefixFieldsFollowTheJavaBoost() throws Exception {
    EntitySpec dataset = harness.registry.getEntitySpec("dataset");
    Map<String, Double> prefixes =
        prefixBoosts(render(SEARCH_TEMPLATE_FULLTEXT_URN, "orders", dataset));
    assertEquals(
        prefixes.keySet(),
        Set.of("name.stemmed", "name.text", "qualifiedName.stemmed", "qualifiedName.text"));
    assertEquals(prefixes.get("name.text"), 26.4d, 0.001d);
    assertEquals(prefixes.get("qualifiedName.text"), 26.4d, 0.001d);
    assertEquals(prefixes.get("name.stemmed"), 13.2d, 0.001d);
    assertEquals(prefixes.get("qualifiedName.stemmed"), 13.2d, 0.001d);

    SearchFieldBoost name = new SearchFieldBoost();
    name.setFieldName("name");
    name.setBoost(10d);
    SearchFieldBoostArray overrides = new SearchFieldBoostArray();
    overrides.add(name);
    SearchFieldOverrides fields = new SearchFieldOverrides();
    fields.setOverride(overrides);
    SearchTemplateProperties heavier = new SearchTemplateProperties();
    heavier.setName("Heavier name");
    heavier.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    heavier.setEditable(true);
    heavier.setParent(UrnUtils.getUrn(SEARCH_TEMPLATE_FULLTEXT_URN));
    heavier.setFields(fields);
    harness.store("urn:li:searchTemplate:heavierName", heavier);
    Map<String, Double> overridden =
        prefixBoosts(render("urn:li:searchTemplate:heavierName", "orders", dataset));
    assertEquals(overridden.get("name.text"), 66.0d, 0.001d);
    assertEquals(overridden.get("qualifiedName.text"), 26.4d, 0.001d);

    Map<String, Double> chart =
        prefixBoosts(
            render(
                SEARCH_TEMPLATE_FULLTEXT_URN, "orders", harness.registry.getEntitySpec("chart")));
    assertEquals(chart.keySet(), prefixes.keySet(), chart.toString());

    ExactMatchConfiguration exact = new ExactMatchConfiguration();
    exact.setWithPrefix(false);
    exact.setPrefixFactor(1.1f);
    exact.setExactFactor(16f);
    SearchConfiguration search = new SearchConfiguration();
    search.setExactMatch(exact);
    OperationContext fulltext =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(SEARCH_TEMPLATE_FULLTEXT_URN, 1L, "fulltext", null));
    String withoutPrefix =
        new SearchV3QueryBuilder(harness.loader, search, null)
            .render(fulltext, List.of(dataset), "orders", true)
            .toString();
    assertFalse(withoutPrefix.contains("match_phrase_prefix"), withoutPrefix);
    OperationContext exactQuery =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate("urn:li:searchTemplate:exact", 1L, "fulltext", null));
    String exactBody =
        new SearchV3QueryBuilder(harness.loader, search, null)
            .render(exactQuery, List.of(dataset), "\"orders\"", true)
            .toString();
    assertEquals(prefixBoosts(exactBody), Map.of("name.text", 1.1d));
  }

  @Test
  public void testShippedQueriesTrackV3IntentClauses() throws Exception {
    EntitySpec dataset = harness.registry.getEntitySpec("dataset");
    String split = render(SEARCH_TEMPLATE_FULLTEXT_URN, "orders2017", dataset);
    assertTrue(split.contains("\"query\" : \"orders~1 2017\""), split);
    assertTrue(split.contains("\"query\" : \"orders2017\""), split);
    String exactName = render(SEARCH_TEMPLATE_FULLTEXT_URN, "orders", dataset);
    JsonNode exactQuery = SearchTemplateFixtures.JSON.readTree(exactName);
    assertTrue(hasTermValue(exactQuery, "name", "orders"));
    assertTrue(hasTermValue(exactQuery, "qualifiedName", "orders"));
    assertFalse(exactName.contains("name.keyword"), exactName);
    assertFalse(exactName.contains("qualifiedName.keyword"), exactName);
    assertFalse(exactName.contains("_entityName"), exactName);
    assertFalse(exactName.contains("constant_score"), exactName);
    String fqn = render(SEARCH_TEMPLATE_FULLTEXT_URN, "my_db.analytics.orders", dataset);
    assertTrue(fqn.contains("qualifiedName.text"), fqn);
    assertFalse(fqn.contains("\"boost\" : 50.0"), fqn);
    assertFalse(fqn.contains("_search."), fqn);
    String phrase =
        render(SEARCH_TEMPLATE_FULLTEXT_URN, "quarterly customer revenue report", dataset);
    assertTrue(phrase.contains("\"boost\" : 100.0"), phrase);
    String identity =
        render(
            "urn:li:searchTemplate:identity",
            "urn:li:dataset:(urn:li:dataPlatform:hive,analytics.orders,PROD)",
            dataset);
    assertTrue(identity.contains("\"boost\" : 100.0"), identity);
    assertTrue(identity.contains("\"id\""), identity);
    assertFalse(identity.contains("id.keyword"), identity);
    assertIdentityFields(identity);
  }

  @Test
  public void testEscapedBackslashDoesNotReviveOperators() throws Exception {
    for (String query : List.of("foo\\*", "foo\\+", "foo\\|")) {
      String decoded = renderedEscaped(query);
      assertFalse(hasLiveOperator(decoded), query + " -> " + decoded);
    }
    String hyphen = renderedEscaped("foo-bar");
    assertTrue(hyphen.contains("\\-"), hyphen);
    assertFalse(hyphen.contains(" "), hyphen);
  }

  @Test
  public void testLightEscapedIdentifierHasNoLiveOperator() throws Exception {
    String light =
        render(
            "urn:li:searchTemplate:light", "foo_bar\\*", harness.registry.getEntitySpec("dataset"));
    List<String> queries = new ArrayList<>();
    collectSimpleQueryText(SearchTemplateFixtures.JSON.readTree(light), queries);
    assertFalse(queries.isEmpty(), light);
    boolean sawIdentifier = false;
    for (String text : queries) {
      assertFalse(hasLiveOperator(text), text);
      if (text.contains("foo_bar")) {
        sawIdentifier = true;
      }
    }
    assertTrue(sawIdentifier, light);
  }

  @Test
  public void testInteractiveSurfaceStartsLightButScrollDoesNot() {
    SearchTemplateProperties search = route("Search", SEARCH_TEMPLATE_FULLTEXT_URN);
    SearchTemplateProperties entry = route("Search light", "urn:li:searchTemplate:light");
    entry.setParent(UrnUtils.getUrn("urn:li:searchTemplate:search"));
    harness.store("urn:li:searchTemplate:search", search);
    harness.store("urn:li:searchTemplate:searchLight", entry);
    harness
        .templates
        .getDefaults()
        .put("fulltext", new HashMap<>(Map.of("default", "urn:li:searchTemplate:search")));
    harness
        .templates
        .getSurfaces()
        .put(
            SearchTemplateSurfaces.SEARCH_ENTITIES,
            new HashMap<>(
                Map.of(
                    "fulltext",
                    new HashMap<>(Map.of("default", "urn:li:searchTemplate:searchLight")))));
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            harness.elastic(),
            null,
            harness.loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    OperationContext interactive =
        coordinator.bind(
            harness.opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES),
            "orders",
            false,
            List.of("dataset"));
    assertEquals(
        interactive.getSearchContext().getResolvedSearchTemplate().getUrn(),
        "urn:li:searchTemplate:light");
    OperationContext scroll =
        coordinator.bind(
            harness.opContext.withSearchSurface(SearchTemplateSurfaces.SCROLL_ENTITIES),
            "orders",
            true,
            List.of("dataset"));
    assertEquals(
        scroll.getSearchContext().getResolvedSearchTemplate().getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);
    OperationContext aggregate =
        coordinator.bind(
            harness.opContext.withSearchSurface(SearchTemplateSurfaces.AGGREGATE_ENTITIES),
            "orders",
            false,
            List.of("dataset"));
    assertEquals(
        aggregate.getSearchContext().getResolvedSearchTemplate().getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);
  }

  /**
   * Shipped seeds have no engine override, so every supported engine uses the default slot. A
   * fixture URN in an engine slot is selected only for that engine.
   */
  @Test
  public void testEngineSlotsAndFixtures() throws Exception {
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            harness.elastic(),
            null,
            harness.loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    ObjectNode root = SearchTemplateFixtures.JSON.createObjectNode();
    ObjectNode shipped = root.putObject("shipped");
    for (SearchClientShim.SearchEngineType engine :
        List.of(
            SearchClientShim.SearchEngineType.ELASTICSEARCH_8,
            SearchClientShim.SearchEngineType.ELASTICSEARCH_9,
            SearchClientShim.SearchEngineType.OPENSEARCH_2,
            SearchClientShim.SearchEngineType.OPENSEARCH_3)) {
      SearchTemplateDryRender preview = preview(coordinator, engine);
      assertEquals(preview.engineSlot(), "default", engine.name());
      assertEquals(preview.resolvedUrn(), SEARCH_TEMPLATE_FULLTEXT_URN, engine.name());
      shipped.put(engineKey(engine), preview.resolvedUrn());
    }

    harness.store("urn:li:searchTemplate:fixtureEs8", marker("ES 8", "es8-only"));
    harness.store("urn:li:searchTemplate:fixtureOs2", marker("OS 2", "os2-only"));
    harness
        .templates
        .getDefaults()
        .get("fulltext")
        .put("elasticsearch8", "urn:li:searchTemplate:fixtureEs8");
    harness
        .templates
        .getDefaults()
        .get("fulltext")
        .put("opensearch2", "urn:li:searchTemplate:fixtureOs2");
    ObjectNode fixtures = root.putObject("fixtures");
    recordFixture(
        fixtures,
        coordinator,
        SearchClientShim.SearchEngineType.ELASTICSEARCH_8,
        "urn:li:searchTemplate:fixtureEs8",
        "es8-only",
        true);
    recordFixture(
        fixtures,
        coordinator,
        SearchClientShim.SearchEngineType.OPENSEARCH_2,
        "urn:li:searchTemplate:fixtureOs2",
        "os2-only",
        true);
    recordFixture(
        fixtures,
        coordinator,
        SearchClientShim.SearchEngineType.OPENSEARCH_3,
        SEARCH_TEMPLATE_FULLTEXT_URN,
        "es8-only",
        false);
    compare("engine-slots.json", canonicalize(root.toString()));
  }

  private void recordFixture(
      ObjectNode fixtures,
      SearchTemplateCoordinator coordinator,
      SearchClientShim.SearchEngineType engine,
      String urn,
      String marker,
      boolean selected) {
    SearchTemplateDryRender preview = preview(coordinator, engine);
    assertEquals(preview.resolvedUrn(), urn, engine.name());
    assertEquals(preview.queryDsl().contains(marker), selected, preview.queryDsl());
    ObjectNode row = fixtures.putObject(engineKey(engine));
    row.put("urn", preview.resolvedUrn());
    row.put("engineSlot", preview.engineSlot());
    row.put("marker", selected);
  }

  private SearchTemplateDryRender preview(
      SearchTemplateCoordinator coordinator, SearchClientShim.SearchEngineType engine) {
    return coordinator.preview(
        harness.opContext,
        SearchTemplateFixtures.FULLTEXT_QUERY,
        List.of("dataset"),
        false,
        engine);
  }

  private static String engineKey(SearchClientShim.SearchEngineType engine) {
    return SearchTemplateDialects.engineKey(engine);
  }

  private static SearchTemplateProperties marker(String name, String value) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(name);
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setApiVersion(SearchTemplateCapabilities.API_VERSION);
    properties.setQuery("{ \"term\": { \"engineMarker\": \"" + value + "\" } }");
    return properties;
  }

  private static SearchTemplateProperties route(String name, String defaultUrn) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(name);
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    SearchTemplateDefaultRoute route = new SearchTemplateDefaultRoute();
    route.setTemplate(UrnUtils.getUrn(defaultUrn));
    properties.setDefaultRoute(route);
    return properties;
  }

  private void assertDatasetFulltext(String query) throws Exception {
    SearchTemplateFixtures.Property certification = harness.property("certification.status");
    assertTrue(query.contains(certification.fieldPath()), certification.fieldPath());
    assertFalse(query.contains(certification.fieldPath() + ".keyword"), query);
    assertFalse(query.contains(harness.property("hidden.label").fieldPath()));
    assertFalse(query.contains(harness.property("notes.internal").fieldPath()));
    assertFalse(query.contains(harness.property("chart.only").fieldPath()));
    assertFalse(query.contains(harness.property("priority.rank").fieldPath()));
    assertFalse(query.contains(harness.property("due.on").fieldPath()));
    assertSharedSearchFields(query);
  }

  private void assertFieldEdits(String query) throws Exception {
    SearchTemplateFixtures.Property hidden = harness.property("hidden.label");
    SearchTemplateFixtures.Property contract = harness.property("contract.id");
    assertTrue(query.contains(hidden.fieldPath()));
    assertFalse(query.contains(hidden.fieldPath() + ".keyword"));
    assertFalse(query.contains(harness.property("certification.status").fieldPath()));
    assertTrue(query.contains(contract.fieldPath()));
    assertTrue(contract.fieldPath().contains("_versioned"));
    assertTrue(contract.fieldPath().contains(contract.version()));
    assertFalse(query.contains("structuredProperties.contract_id.keyword"));
    assertSharedSearchFields(query);
    String named =
        render(
            SearchTemplateFixtures.FIELD_EDITS,
            "contract id gold",
            harness.registry.getEntitySpec("dataset"));
    assertTrue(named.contains("\"boost\" : 9.0"));
    assertTrue(named.contains("\"boost\" : 8.0"));
    assertTrue(named.contains("\"gold\""));
  }

  private void assertNameAndValue(String query) {
    assertTrue(query.contains("\"tie_breaker\" : 0.0"));
    assertTrue(query.contains(harness.property("certification.status").fieldPath()));
    assertFalse(query.contains(harness.property("certification.status").fieldPath() + ".keyword"));
    assertTrue(query.contains("\"gold\""));
    assertTrue(query.contains("\"boost\" : 8.0"));
    assertTrue(query.contains("\"boost\" : 3.0"));
    assertFalse(query.contains(harness.property("chart.only").fieldPath()));
  }

  private void assertSharedSearchFields(String query) throws Exception {
    JsonNode root = SearchTemplateFixtures.JSON.readTree(query);
    List<String> fields = new ArrayList<>();
    collectSimpleQueryFields(root, fields);
    assertTrue(
        fields.stream().anyMatch(field -> field.startsWith("name.text^")), fields.toString());
    assertTrue(
        fields.stream()
            .anyMatch(
                field -> field.startsWith("structuredProperties.") && field.contains(".text^")),
        fields.toString());
    assertTrue(fields.stream().noneMatch(field -> field.contains("_search.")), fields.toString());
  }

  private static String fieldRoot(String field) {
    String bare = field.replaceAll("\\^.*$", "");
    if (bare.startsWith("structuredProperties.")) {
      return bare.replaceAll("\\.(text|keyword|stemmed|delimited)$", "");
    }
    int dot = bare.indexOf('.');
    return dot < 0 ? bare : bare.substring(0, dot);
  }

  private EntitySpec datasetSpec() {
    return harness.registry.getEntitySpec("dataset");
  }

  private Set<String> sharedFields() {
    return V3SearchFields.fullTextFields(
            V3SearchFields.indexGroupSpecs(harness.registry, List.of(datasetSpec())))
        .keySet();
  }

  private List<String> datasetIndexRoots() throws Exception {
    EntitySpec dataset = datasetSpec();
    EntityIndexConfiguration index = harness.elastic().getEntityIndex();
    index.getV3().setMappingConfig("search_entity_mapping_config.yaml");
    Map<String, Object> generated =
        new MultiEntityMappingsBuilder(index, null, ESUtils.KEYWORD_MAXLENGTH, List.of())
            .getIndexMappings(harness.registry, dataset);
    Map<String, Object> merged =
        MultiEntityMappingsUtils.mergeMappings(
            generated,
            MultiEntityMappingsUtils.loadMappingConfigurationFromResource(
                "search_entity_mapping_config.yaml"));
    Object properties = merged.get("properties");
    if (!(properties instanceof Map<?, ?> map)) {
      return List.of();
    }
    return map.keySet().stream().map(String::valueOf).sorted().toList();
  }

  private void assertIdentityFields(String identity) throws Exception {
    JsonNode root = SearchTemplateFixtures.JSON.readTree(identity);
    assertTrue(hasTermField(root, "urn"), "urn");
    assertTrue(hasTermField(root, "urn.lowercase"), "urn.lowercase");
    assertTrue(hasTermValue(root, "urn_name.lowercase", "analytics.orders"), "urn_name.lowercase");
    Set<String> referenced = new LinkedHashSet<>();
    collectReferencedFields(root, referenced);
    assertFalse(
        referenced.stream()
            .map(field -> field.replaceAll("\\^.*$", ""))
            .anyMatch("urn_name.text"::equals),
        referenced.toString());
    Map<String, Object> urnName =
        Map.of(MappingConstants.URN_NAME_FIELD, V3SearchFields.fullUrnRootMapping());
    assertTrue(mappingContains(urnName, "urn_name.lowercase"));
    assertFalse(mappingContains(urnName, "urn_name.text"));
    Map<String, Object> properties = datasetMappingProperties();
    for (String path : List.of("urn", "urn.lowercase", "urn_name", "urn_name.lowercase")) {
      assertTrue(mappingContains(properties, path), path);
    }
  }

  private static boolean hasTermField(JsonNode node, String field) {
    return hasTermValue(node, field, null);
  }

  private static boolean hasTermValue(JsonNode node, String field, String value) {
    if (node == null) {
      return false;
    }
    if (node.isObject()) {
      JsonNode term = node.get("term");
      if (term != null && term.isObject() && term.has(field)) {
        if (value == null) {
          return true;
        }
        JsonNode body = term.get(field);
        if (body.isTextual()) {
          return value.equals(body.asText());
        }
        return value.equals(body.path("value").asText());
      }
      var fields = node.fields();
      while (fields.hasNext()) {
        if (hasTermValue(fields.next().getValue(), field, value)) {
          return true;
        }
      }
    } else if (node.isArray()) {
      for (JsonNode child : node) {
        if (hasTermValue(child, field, value)) {
          return true;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> datasetMappingProperties() throws Exception {
    EntityIndexConfiguration index = harness.elastic().getEntityIndex();
    index.getV3().setMappingConfig("search_entity_mapping_config.yaml");
    for (MappingsBuilder.IndexMapping mapping :
        new MultiEntityMappingsBuilder(index, null, ESUtils.KEYWORD_MAXLENGTH, List.of())
            .getIndexMappings(harness.opContext, List.of())) {
      Object properties = mapping.getMappings().get("properties");
      if (properties instanceof Map<?, ?> map && map.containsKey("qualifiedName")) {
        return (Map<String, Object>) map;
      }
    }
    return Map.of();
  }

  private static boolean mappingContains(Map<String, Object> properties, String path) {
    return mappingContains(properties, path.split("\\."), 0);
  }

  @SuppressWarnings("unchecked")
  private static boolean mappingContains(
      Map<String, Object> properties, String[] parts, int index) {
    Object node = properties.get(parts[index]);
    if (!(node instanceof Map<?, ?> raw)) {
      return false;
    }
    if (index == parts.length - 1) {
      return true;
    }
    Map<String, Object> map = (Map<String, Object>) raw;
    Object fields = map.get("fields");
    if (fields instanceof Map<?, ?> fieldMap
        && index + 1 == parts.length - 1
        && fieldMap.containsKey(parts[index + 1])) {
      return true;
    }
    Object nested = map.get("properties");
    if (nested instanceof Map<?, ?> nestedMap) {
      return mappingContains((Map<String, Object>) nestedMap, parts, index + 1);
    }
    return false;
  }

  private static void collectReferencedFields(JsonNode node, Set<String> fields) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      for (String clause : List.of("simple_query_string", "multi_match", "query_string")) {
        JsonNode body = node.get(clause);
        if (body != null && body.get("fields") != null && body.get("fields").isArray()) {
          body.get("fields").forEach(field -> fields.add(field.asText()));
        }
      }
      for (String clause : List.of("term", "match", "match_phrase", "match_phrase_prefix")) {
        JsonNode body = node.get(clause);
        if (body != null && body.isObject()) {
          body.fieldNames()
              .forEachRemaining(
                  name -> {
                    if (!name.equals("boost") && !name.equals("_name")) {
                      fields.add(name);
                    }
                  });
        }
      }
      node.fields().forEachRemaining(entry -> collectReferencedFields(entry.getValue(), fields));
    } else if (node.isArray()) {
      node.forEach(child -> collectReferencedFields(child, fields));
    }
  }

  private static String renderedEscaped(String query) throws Exception {
    Map<String, Object> context = new LinkedHashMap<>();
    context.put(SearchTemplateContextVariables.ESCAPED, SearchTemplateQueryShape.of(query).escaped);
    String rendered =
        SearchTemplateMustache.render(
            "{ \"simple_query_string\": { \"query\": \"{{escaped}}\" } }", context);
    return SearchTemplateFixtures.JSON
        .readTree(rendered)
        .get("simple_query_string")
        .get("query")
        .asText();
  }

  private static void collectSimpleQueryText(JsonNode node, List<String> queries) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      JsonNode simple = node.get("simple_query_string");
      if (simple != null && simple.hasNonNull("query")) {
        queries.add(simple.get("query").asText());
      }
      node.fields().forEachRemaining(entry -> collectSimpleQueryText(entry.getValue(), queries));
    } else if (node.isArray()) {
      node.forEach(child -> collectSimpleQueryText(child, queries));
    }
  }

  /**
   * A live operator has an even number of preceding backslashes, so the escape does not cover it.
   */
  private static boolean hasLiveOperator(String text) {
    String operators = "+|-*()~\"";
    for (int i = 0; i < text.length(); i++) {
      if (operators.indexOf(text.charAt(i)) < 0) {
        continue;
      }
      int slashes = 0;
      for (int j = i - 1; j >= 0 && text.charAt(j) == '\\'; j--) {
        slashes++;
      }
      if (slashes % 2 == 0) {
        return true;
      }
    }
    return false;
  }

  private static Map<String, Double> prefixBoosts(String query) throws Exception {
    Map<String, Double> boosts = new LinkedHashMap<>();
    collectPrefixBoosts(SearchTemplateFixtures.JSON.readTree(query), boosts);
    return boosts;
  }

  private static void collectPrefixBoosts(JsonNode node, Map<String, Double> boosts) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      JsonNode prefix = node.get("match_phrase_prefix");
      if (prefix != null && prefix.isObject()) {
        prefix
            .fields()
            .forEachRemaining(
                entry -> {
                  if (entry.getValue().has("boost")) {
                    boosts.put(entry.getKey(), entry.getValue().get("boost").asDouble());
                  }
                });
      }
      node.fields().forEachRemaining(entry -> collectPrefixBoosts(entry.getValue(), boosts));
    } else if (node.isArray()) {
      node.forEach(child -> collectPrefixBoosts(child, boosts));
    }
  }

  private static void collectSimpleQueryFields(JsonNode node, List<String> fields) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      JsonNode simple = node.get("simple_query_string");
      if (simple != null && simple.get("fields") != null && simple.get("fields").isArray()) {
        simple.get("fields").forEach(field -> fields.add(field.asText()));
      }
      node.fields().forEachRemaining(entry -> collectSimpleQueryFields(entry.getValue(), fields));
    } else if (node.isArray()) {
      node.forEach(child -> collectSimpleQueryFields(child, fields));
    }
  }

  private String render(String urn, String query, EntitySpec spec) {
    OperationContext bound =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(urn, 1L, "fulltext", null));
    QueryBuilder rendered =
        new SearchV3QueryBuilder(harness.loader, new SearchConfiguration(), null)
            .render(bound, List.of(spec), query, true);
    return rendered.toString();
  }

  private ObjectNode manifest() throws Exception {
    ObjectNode root = SearchTemplateFixtures.JSON.createObjectNode();
    ArrayNode catalog = root.putArray("catalog");
    Set<String> datasetFields = new LinkedHashSet<>();
    for (SearchTemplateFixtures.Property property : harness.properties) {
      if (property.queryByDefault()
          && property.entityTypes().contains("dataset")
          && Set.of("string", "rich_text", "urn").contains(property.valueType())) {
        datasetFields.add(property.fieldPath());
      }
    }
    for (SearchTemplateFixtures.Property property : harness.properties) {
      ObjectNode row = catalog.addObject();
      row.put("qualifiedName", property.qualifiedName());
      row.put("fieldPath", property.fieldPath());
      row.put("valueType", property.valueType());
      ArrayNode types = row.putArray("entityTypes");
      property.entityTypes().forEach(types::add);
      row.put("queryByDefault", property.queryByDefault());
      row.put("boostScore", property.boostScore());
      if (property.displayName() != null) {
        row.put("displayName", property.displayName());
      }
      if (property.version() != null) {
        row.put("version", property.version());
      }
      boolean included = datasetFields.contains(property.fieldPath());
      boolean wholeValue =
          property.valueType().equals("string")
              || property.valueType().equals("rich_text")
              || property.valueType().equals("urn");
      String datasetSearch;
      if (!property.queryByDefault() || !property.entityTypes().contains("dataset")) {
        datasetSearch = "leftOut";
      } else if (!wholeValue) {
        datasetSearch = "notWholeValue";
      } else if (included) {
        datasetSearch = "valueTerm";
      } else {
        datasetSearch = "leftOut";
      }
      row.put("datasetSearch", datasetSearch);
    }
    ArrayNode indexRoots = root.putArray("datasetIndexRoots");
    datasetIndexRoots().forEach(indexRoots::add);
    ArrayNode sharedFields = root.putArray("datasetSharedFields");
    sharedFields().stream().sorted().forEach(sharedFields::add);
    ObjectNode entities = root.putObject("entities");
    List<EntitySpec> specs = new ArrayList<>(harness.registry.getEntitySpecs().values());
    specs.sort(Comparator.comparing(EntitySpec::getName));
    for (EntitySpec spec : specs) {
      ArrayNode fields = entities.putObject(spec.getName()).putArray("queryFields");
      for (SearchFieldConfig field : SearchTemplateFields.pdlQueryFields(List.of(spec))) {
        ObjectNode entry = fields.addObject();
        entry.put("field", field.fieldName());
        entry.put("boost", field.boost() == null ? 1.0 : field.boost().doubleValue());
      }
    }
    return root;
  }

  private static void compare(String name, String actual) throws Exception {
    String expected = readGolden(name);
    if ("1".equals(System.getenv("UPDATE_SEARCH_TEMPLATE_GOLDEN"))) {
      Files.createDirectories(goldenDir());
      Files.writeString(goldenDir().resolve(name), actual);
      return;
    }
    if (expected == null) {
      fail(IMPACT + " Missing golden " + name);
    }
    assertEquals(actual, expected, IMPACT);
  }

  private static String canonicalize(String json) throws Exception {
    return SearchTemplateFixtures.JSON
            .writerWithDefaultPrettyPrinter()
            .writeValueAsString(sort(SearchTemplateFixtures.JSON.readTree(json)))
        + "\n";
  }

  private static JsonNode sort(JsonNode node) {
    if (node.isObject()) {
      ObjectNode sorted = SearchTemplateFixtures.JSON.createObjectNode();
      TreeMap<String, JsonNode> fields = new TreeMap<>();
      node.fields().forEachRemaining(entry -> fields.put(entry.getKey(), entry.getValue()));
      fields.forEach((key, value) -> sorted.set(key, sort(value)));
      return sorted;
    }
    if (node.isArray()) {
      ArrayNode sorted = SearchTemplateFixtures.JSON.createArrayNode();
      node.forEach(child -> sorted.add(sort(child)));
      return sorted;
    }
    return node;
  }

  private static String readGolden(String name) throws Exception {
    Path path = goldenDir().resolve(name);
    if (!Files.exists(path)) {
      return null;
    }
    return Files.readString(path);
  }

  /**
   * Where a one-token dataset query puts each isolated field. Lineage fulltext is the fulltext
   * body.
   */
  @Test
  public void testDefaultQueryFieldPlacement() throws Exception {
    EntitySpec dataset = harness.registry.getEntitySpec("dataset");
    Map<String, String> bodies = new LinkedHashMap<>();
    bodies.put("light", render("urn:li:searchTemplate:light", "orders", dataset));
    bodies.put("fulltext", render(SEARCH_TEMPLATE_FULLTEXT_URN, "orders", dataset));
    bodies.put("exact", render("urn:li:searchTemplate:exact", "orders", dataset));
    bodies.put("structured", render("urn:li:searchTemplate:structured", "orders", dataset));
    bodies.put("lineage", render(SEARCH_TEMPLATE_FULLTEXT_URN, "orders", dataset));
    Map<String, Map<String, String>> actual = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : bodies.entrySet()) {
      actual.put(entry.getKey(), placeFields(entry.getValue()));
    }
    assertEquals(
        actual.get("lineage"), actual.get("fulltext"), "lineage fulltext is the fulltext body");
    assertEquals(actual, expectedFieldPlacement());
  }

  private static Map<String, Map<String, String>> expectedFieldPlacement() {
    Map<String, Map<String, String>> expected = new LinkedHashMap<>();
    expected.put(
        "light",
        Map.of(
            "name_1", "absent",
            "description_1", "absent",
            "tags", "absent",
            "glossaryTerms", "absent",
            "other", "absent",
            ".stemmed", "field list"));
    Map<String, String> fulltext =
        Map.of(
            "name_1", "absent",
            "description_1", "absent",
            "tags", "field list",
            "glossaryTerms", "field list",
            "other", "field list",
            ".stemmed", "dedicated and field list");
    expected.put("fulltext", fulltext);
    expected.put(
        "exact",
        Map.of(
            "name_1", "absent",
            "description_1", "absent",
            "tags", "absent",
            "glossaryTerms", "absent",
            "other", "absent",
            ".stemmed", "absent"));
    expected.put(
        "structured",
        Map.of(
            "name_1", "absent",
            "description_1", "absent",
            "tags", "field list",
            "glossaryTerms", "field list",
            "other", "field list",
            ".stemmed", "field list"));
    expected.put("lineage", fulltext);
    return expected;
  }

  private static Map<String, String> placeFields(String rendered) throws Exception {
    Set<String> listed = new LinkedHashSet<>();
    Set<String> dedicated = new LinkedHashSet<>();
    collectPlacement(SearchTemplateFixtures.JSON.readTree(rendered), listed, dedicated);
    Map<String, String> placed = new LinkedHashMap<>();
    for (String field :
        List.of("name_1", "description_1", "tags", "glossaryTerms", "other", ".stemmed")) {
      placed.put(field, placement(field, listed, dedicated));
    }
    return placed;
  }

  private static void collectPlacement(JsonNode node, Set<String> listed, Set<String> dedicated) {
    if (node == null || node.isNull()) {
      return;
    }
    if (node.isObject()) {
      for (String clause : List.of("simple_query_string", "multi_match", "query_string")) {
        JsonNode body = node.get(clause);
        if (body != null && body.get("fields") != null && body.get("fields").isArray()) {
          body.get("fields").forEach(field -> listed.add(stripBoost(field.asText())));
        }
      }
      for (String clause :
          List.of("term", "match", "match_phrase", "match_phrase_prefix", "prefix")) {
        JsonNode body = node.get(clause);
        if (body != null && body.isObject()) {
          body.fieldNames()
              .forEachRemaining(
                  name -> {
                    if (!name.equals("boost")
                        && !name.equals("_name")
                        && !name.equals("query")
                        && !name.equals("value")) {
                      dedicated.add(name);
                    }
                  });
        }
      }
      node.fields()
          .forEachRemaining(entry -> collectPlacement(entry.getValue(), listed, dedicated));
    } else if (node.isArray()) {
      node.forEach(child -> collectPlacement(child, listed, dedicated));
    }
  }

  private static String placement(String kind, Set<String> listed, Set<String> dedicated) {
    boolean inList = listed.stream().anyMatch(field -> fieldMatches(kind, field));
    boolean ownClause = dedicated.stream().anyMatch(field -> fieldMatches(kind, field));
    if (ownClause && inList) {
      return "dedicated and field list";
    }
    if (ownClause) {
      return "dedicated";
    }
    if (inList) {
      return "field list";
    }
    return "absent";
  }

  private static boolean fieldMatches(String kind, String field) {
    return switch (kind) {
      case "name_1" -> field.equals("name_1") || field.startsWith("name_1.");
      case "description_1" -> field.equals("description_1") || field.startsWith("description_1.");
      case "tags" -> field.equals("tags") || field.startsWith("tags.");
      case "glossaryTerms" -> field.equals("glossaryTerms") || field.startsWith("glossaryTerms.");
      case "other" -> field.equals("other") || field.startsWith("other.");
      case ".stemmed" -> stemmedTextField(field);
      default -> false;
    };
  }

  private static boolean stemmedTextField(String field) {
    if (!field.endsWith(".stemmed")) {
      return false;
    }
    String root = field.substring(0, field.length() - ".stemmed".length());
    return Set.of(
            "name",
            "qualifiedName",
            "description",
            "other",
            "tags",
            "glossaryTerms",
            "name_1",
            "description_1")
        .contains(root);
  }

  private static String stripBoost(String field) {
    int caret = field.indexOf('^');
    return caret < 0 ? field : field.substring(0, caret);
  }

  private static Path goldenDir() {
    Path module = Path.of("metadata-io/src/test/resources/search/template");
    if (Files.isDirectory(module) || Files.isDirectory(module.getParent())) {
      return module;
    }
    return Path.of("src/test/resources/search/template");
  }
}
