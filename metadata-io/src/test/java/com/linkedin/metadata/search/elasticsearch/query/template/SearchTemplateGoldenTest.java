package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.v3.StructuredPropertyQueryFields;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.opensearch.index.query.QueryBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Renders the shipped templates against the real entity registry and the committed property
 * catalog. A schema or search-option change fails here so the author can see the search impact.
 */
public class SearchTemplateGoldenTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
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

  @AfterMethod
  public void clearCatalog() {
    StructuredPropertyQueryFields.replace(List.of());
    SearchTemplates.install(null);
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
    compare("dataset-fulltext.json", canonicalize(fulltext));
    compare("dataset-structured.json", canonicalize(structured));
    compare("dataset-field-edits.json", canonicalize(fieldEdits));
    compare("dataset-name-and-value.json", canonicalize(nameAndValue));
    compare("field-manifest.json", canonicalize(manifest().toString()));
  }

  @Test
  public void testShippedQueriesTrackV3IntentClauses() {
    EntitySpec dataset = harness.registry.getEntitySpec("dataset");
    String split = render(SEARCH_TEMPLATE_FULLTEXT_URN, "orders2017", dataset);
    assertTrue(split.contains("\"query\" : \"orders~1 2017\""), split);
    assertTrue(split.contains("\"query\" : \"orders2017\""), split);
    String fqn = render(SEARCH_TEMPLATE_FULLTEXT_URN, "my_db.analytics.orders", dataset);
    assertTrue(fqn.contains("\"boost\" : 50.0"), fqn);
    assertTrue(fqn.contains("qualifiedName.text"), fqn);
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
    assertTrue(identity.contains("id.keyword"), identity);
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
            SearchTemplateSurfaces.SEARCH,
            new HashMap<>(
                Map.of(
                    "fulltext",
                    new HashMap<>(Map.of("default", "urn:li:searchTemplate:searchLight")))));
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(harness.elastic(), null, harness.loader);
    OperationContext interactive =
        coordinator.bind(
            harness.opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH),
            "orders",
            false,
            List.of("dataset"));
    assertEquals(
        interactive.getSearchContext().getResolvedSearchTemplate().getUrn(),
        "urn:li:searchTemplate:light");
    OperationContext scroll =
        coordinator.bind(
            harness.opContext.withSearchSurface(SearchTemplateSurfaces.SCROLL),
            "orders",
            true,
            List.of("dataset"));
    assertEquals(
        scroll.getSearchContext().getResolvedSearchTemplate().getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);
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
    assertTrue(query.contains(certification.fieldPath() + ".keyword"), certification.fieldPath());
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
    assertTrue(query.contains(hidden.fieldPath() + ".keyword"));
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
    assertTrue(query.contains(harness.property("certification.status").fieldPath() + ".keyword"));
    assertTrue(query.contains("\"gold\""));
    assertTrue(query.contains("\"boost\" : 8.0"));
    assertTrue(query.contains("\"boost\" : 3.0"));
    assertFalse(query.contains(harness.property("chart.only").fieldPath()));
  }

  private void assertSharedSearchFields(String query) throws Exception {
    JsonNode root = MAPPER.readTree(query);
    List<String> fields = new ArrayList<>();
    collectSimpleQueryFields(root, fields);
    assertTrue(
        fields.stream().anyMatch(field -> field.startsWith("name.text^")), fields.toString());
    assertTrue(
        fields.stream().noneMatch(field -> field.startsWith("structuredProperties.")),
        fields.toString());
    assertTrue(fields.stream().noneMatch(field -> field.contains("_search.")), fields.toString());
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

  private ObjectNode manifest() {
    ObjectNode root = MAPPER.createObjectNode();
    ArrayNode catalog = root.putArray("catalog");
    Set<String> datasetFields = new LinkedHashSet<>();
    for (SearchFieldConfig field : StructuredPropertyQueryFields.fieldsFor(List.of("dataset"))) {
      datasetFields.add(field.fieldName());
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
      row.put("fullTextSearch", property.fullTextSearch());
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
    return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(sort(MAPPER.readTree(json)))
        + "\n";
  }

  private static JsonNode sort(JsonNode node) {
    if (node.isObject()) {
      ObjectNode sorted = MAPPER.createObjectNode();
      TreeMap<String, JsonNode> fields = new TreeMap<>();
      node.fields().forEachRemaining(entry -> fields.put(entry.getKey(), entry.getValue()));
      fields.forEach((key, value) -> sorted.set(key, sort(value)));
      return sorted;
    }
    if (node.isArray()) {
      ArrayNode sorted = MAPPER.createArrayNode();
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

  private static Path goldenDir() {
    Path module = Path.of("metadata-io/src/test/resources/search/template");
    if (Files.isDirectory(module) || Files.isDirectory(module.getParent())) {
      return module;
    }
    return Path.of("src/test/resources/search/template");
  }
}
