package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.DESCRIPTION;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.ENTITY_NAME;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.OTHER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.QUALIFIED_NAME;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class V3SearchFieldsTest {

  private static final EntityRegistry REGISTRY = TestOperationContexts.defaultEntityRegistry();
  private static final List<String> COLUMN_ARRAYS = List.copyOf(V3SearchFields.COLUMN_ARRAY_FIELDS);

  @Test
  public void testDatasetFullTextFieldsAndTheirSources() {
    Map<String, List<String>> fields = V3SearchFields.fullTextFields(List.of(dataset()));

    assertEquals(
        fields.keySet(),
        Set.of(ENTITY_NAME, QUALIFIED_NAME, DESCRIPTION, OTHER, "tags", "glossaryTerms"));
    // Matched fields follow this order, so the catch-all comes last
    assertEquals(List.copyOf(fields.keySet()).get(fields.size() - 1), OTHER);
    assertTrue(fields.get(ENTITY_NAME).contains("name"), fields.toString());
    assertTrue(
        fields.get(ENTITY_NAME).contains("_aspects.editableDatasetProperties.editedName"),
        fields.toString());
    assertEquals(fields.get(QUALIFIED_NAME), List.of("qualifiedName"));
    assertTrue(fields.get(DESCRIPTION).contains("description"), fields.toString());
    assertTrue(
        fields.get(DESCRIPTION).contains("_aspects.editableDatasetProperties.editedDescription"),
        fields.toString());
    assertFalse(fields.get(DESCRIPTION).contains("fieldDescriptions"), fields.toString());
    assertFalse(fields.containsKey("description_1"), fields.toString());
    assertFalse(fields.containsKey("name_1"), fields.toString());
    assertFalse(fields.containsKey("columns"), fields.toString());
    assertEquals(fields.get("tags"), List.of("tags"), fields.toString());
    assertTrue(
        V3SearchFields.storedParsedNameFields(List.of(dataset()))
            .get("tags")
            .contains("_aspects.globalTags.tags__urn_name"),
        V3SearchFields.storedParsedNameFields(List.of(dataset())).toString());
    assertEquals(fields.get("glossaryTerms"), List.of("glossaryTerms"), fields.toString());
    assertFalse(fields.containsKey("owners"), fields.toString());
    assertFalse(fields.containsKey("platform"), fields.toString());
    // Reference fields stay out of the default query and copy their names into other
    assertTrue(
        fields
            .get(OTHER)
            .containsAll(
                List.of(
                    "platform",
                    "container",
                    "domains",
                    "dataProduct",
                    "platformInstance",
                    "_aspects.applications.applications__urn_name")),
        fields.get(OTHER).toString());
    assertFalse(fields.get(OTHER).contains("tags"), fields.toString());
    // A field that feeds a named shared field does not also land in other
    assertTrue(
        fields.get(OTHER).stream()
            .noneMatch(field -> List.of("name", "description", "fieldPaths").contains(field)),
        fields.toString());
  }

  /**
   * A search that includes both entities still skips column text. The business attribute's own
   * description opts in {@code description}; that does not opt in {@code description_1}.
   */
  @Test
  public void testSharedSourceNameDoesNotOptInAnotherDegree() {
    Map<String, List<String>> fields =
        V3SearchFields.fullTextFields(
            List.of(dataset(), REGISTRY.getEntitySpec("businessAttribute")));

    assertTrue(fields.containsKey(DESCRIPTION), fields.toString());
    assertTrue(fields.get(DESCRIPTION).contains("description"), fields.toString());
    assertTrue(
        fields.get(DESCRIPTION).contains("_aspects.editableDatasetProperties.editedDescription"),
        fields.toString());
    assertTrue(
        V3SearchFields.omitsSourceRoot(
            "editedFieldDescriptions",
            List.of(dataset(), REGISTRY.getEntitySpec("businessAttribute"))),
        fields.toString());
    assertFalse(fields.containsKey("description_1"), fields.toString());
    assertFalse(fields.containsKey("name_1"), fields.toString());
    assertFalse(fields.containsKey("tags_1"), fields.toString());
    assertFalse(fields.containsKey("glossaryTerms_1"), fields.toString());
  }

  /** An unqualified dataset search does not read column fields, and never fetches their arrays. */
  @Test
  public void testMatchedFieldSourcesLeaveOutTheColumnArrays() {
    Map<String, List<String>> fields = V3SearchFields.fullTextFields(List.of(dataset()));
    assertFalse(fields.containsKey("name_1"), fields.toString());

    List<String> sources = V3SearchFields.matchedFieldSources(fields);

    assertTrue(
        sources.containsAll(List.of("name", "qualifiedName", "description")), sources.toString());
    assertTrue(sources.stream().noneMatch(COLUMN_ARRAYS::contains), sources.toString());
    // Nor wherever a label sends one
    assertEquals(
        V3SearchFields.matchedFieldSources(
            Map.of(
                DESCRIPTION,
                List.of(
                    "description",
                    "fieldDescriptions",
                    "_aspects.editableSchemaMetadata.editedFieldDescriptions"))),
        List.of("description"));
  }

  /**
   * Of other, matched fields fetch only what names the entity: a document's text, custom properties
   * and lists of URNs are not full-text fields and could be any size.
   */
  @Test
  public void testMatchedFieldSourcesKeepOnlyNamesAndTagsOfOther() {
    List<String> datasetSources =
        V3SearchFields.matchedFieldSources(V3SearchFields.fullTextFields(List.of(dataset())));
    assertTrue(datasetSources.contains("name"), datasetSources.toString());
    assertFalse(datasetSources.contains("editedName"), datasetSources.toString());
    assertTrue(datasetSources.contains("tags"), datasetSources.toString());
    assertTrue(datasetSources.contains("glossaryTerms"), datasetSources.toString());
    assertFalse(datasetSources.contains("platform"), datasetSources.toString());
    assertTrue(
        datasetSources.stream().noneMatch(List.of("customProperties", "domains")::contains),
        datasetSources.toString());

    Map<String, List<String>> documentFields =
        V3SearchFields.fullTextFields(List.of(REGISTRY.getEntitySpec("document")));
    List<String> freeText = List.of("text", "semanticText", "customProperties");
    assertTrue(documentFields.get(OTHER).containsAll(freeText), documentFields.toString());
    List<String> documentSources = V3SearchFields.matchedFieldSources(documentFields);
    assertTrue(documentSources.contains("_aspects.documentInfo.title"), documentSources.toString());
    assertFalse(documentSources.contains("title"), documentSources.toString());
    assertFalse(documentSources.contains("name"), documentSources.toString());
    assertTrue(documentSources.stream().noneMatch(freeText::contains), documentSources.toString());
    assertTrue(
        datasetSources.stream().noneMatch(V3SearchFields.COLUMN_ARRAY_FIELDS::contains),
        datasetSources.toString());
    assertTrue(
        datasetSources.stream()
            .noneMatch(
                source ->
                    V3SearchFields.COLUMN_ARRAY_FIELDS.contains(
                        source.substring(source.lastIndexOf('.') + 1))),
        datasetSources.toString());
  }

  /** Chart and dashboard title share a spec by value and live on different aspects. */
  @Test
  public void testRelabeledFieldKeepsTheAspectThatDeclaresIt() {
    List<String> sources =
        V3SearchFields.fullTextFields(
                List.of(REGISTRY.getEntitySpec("chart"), REGISTRY.getEntitySpec("dashboard")))
            .get(ENTITY_NAME);
    assertTrue(sources.contains("_aspects.chartInfo.title"), sources.toString());
    assertTrue(sources.contains("_aspects.dashboardInfo.title"), sources.toString());
  }

  /**
   * Across every entity and every shared index, matched fields fetch single values only, besides
   * the short lists of tags and glossary terms: an array a label sends to a named shared field
   * would be fetched whole with every hit.
   */
  @Test
  public void testMatchedFieldSourcesAreSingleValuedAcrossTheRegistry() {
    Set<List<EntitySpec>> indexGroups =
        REGISTRY.getEntitySpecs().values().stream()
            .map(entitySpec -> V3SearchFields.indexGroupSpecs(REGISTRY, List.of(entitySpec)))
            .collect(Collectors.toSet());
    for (List<EntitySpec> group : indexGroups) {
      Map<String, List<SearchableFieldSpec>> roots = V3SearchFields.rootSourceFieldSpecs(group);
      List<String> arrays =
          V3SearchFields.matchedFieldSources(V3SearchFields.fullTextFields(group)).stream()
              .filter(source -> !List.of("tags", "glossaryTerms").contains(source))
              .filter(
                  source ->
                      roots.getOrDefault(source, List.of()).stream()
                          .anyMatch(SearchableFieldSpec::isArray))
              .toList();
      assertTrue(arrays.isEmpty(), group.get(0).getName() + ": " + arrays);
    }
  }

  @Test
  public void testLabelWinsOverTheOtherFallback() {
    SearchableFieldSpec labeled = fieldSpec(FieldType.TEXT, "name", false);
    SearchableFieldSpec unlabeled = fieldSpec(FieldType.TEXT, null, false);

    // Two sources of one root field: the root copies into the label only
    assertEquals(
        V3SearchFields.destinations(List.of(labeled, unlabeled), Set.of()), Set.of(ENTITY_NAME));
    assertEquals(V3SearchFields.destinations(List.of(unlabeled), Set.of()), Set.of(OTHER));
    assertEquals(
        V3SearchFields.destinations(List.of(fieldSpec(FieldType.TEXT, null, true)), Set.of()),
        Set.of(OTHER));
    // A keyword not queried by default feeds no shared field
    assertEquals(
        V3SearchFields.destinations(List.of(fieldSpec(FieldType.KEYWORD, null, false)), Set.of()),
        Set.of());
  }

  /**
   * Service displayName is labeled name, so full text reads name and omits the displayName root.
   */
  @Test
  public void testEntityWithoutNameLabelSendsItsEntityNameAliasToEntityName() {
    EntitySpec service = REGISTRY.getEntitySpec("service");

    Map<String, List<String>> fields = V3SearchFields.fullTextFields(List.of(service));

    assertEquals(fields.get(ENTITY_NAME), List.of("_aspects.serviceProperties.displayName"));
    assertFalse(fields.get(OTHER).contains("displayName"), fields.toString());
    assertTrue(V3SearchFields.omitsSourceRoot("displayName", List.of(service)));
  }

  @Test
  public void testMappingShapes() {
    Map<String, Object> text =
        Map.of("type", "text", "analyzer", "v3_text", "search_analyzer", "v3_text_search");
    Map<String, Object> stemmed =
        Map.of("type", "text", "analyzer", "v3_stemmed", "search_analyzer", "v3_stemmed_search");

    Map<String, Object> indexedFullText =
        Map.of(
            "type",
            "keyword",
            "ignore_above",
            8191,
            "fields",
            Map.of("text", text, "stemmed", stemmed));
    // Model labels are not shaped by name. The built-in helper's indexed form keeps a keyword.
    assertEquals(V3SearchFields.mapping(ENTITY_NAME, true), indexedFullText);
    assertEquals(V3SearchFields.mapping(DESCRIPTION, true), indexedFullText);
    // other and structuredProperties are search-only: the root is not an exact field
    assertEquals(
        V3SearchFields.mapping(V3SearchFields.OTHER, true),
        Map.of(
            "type",
            "keyword",
            "index",
            false,
            "doc_values",
            false,
            "fields",
            Map.of("text", text, "stemmed", stemmed)));
    assertEquals(V3SearchFields.mapping("owner", true), indexedFullText);
    // A label no full-text field feeds stays a plain keyword
    assertEquals(
        V3SearchFields.mapping("origin", false), Map.of("type", "keyword", "ignore_above", 8191));
  }

  /** A search of one entity of a grouped index reads the shared fields its whole group writes. */
  @Test
  public void testGroupedEntitiesReadTheSharedFieldsOfTheirIndex() {
    EntitySpec dataset = spy(dataset());
    EntitySpec role = spy(REGISTRY.getEntitySpec("dataHubRole"));
    EntitySpec chart = REGISTRY.getEntitySpec("chart");
    doReturn("shared").when(dataset).getSearchGroup();
    doReturn("shared").when(role).getSearchGroup();
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.getEntitySpecs())
        .thenReturn(Map.of("dataset", dataset, "dataHubRole", role, "chart", chart));

    List<EntitySpec> roleIndex = V3SearchFields.indexGroupSpecs(registry, List.of(role));
    assertEquals(Set.copyOf(roleIndex), Set.of(dataset, role));
    assertEquals(V3SearchFields.indexGroupSpecs(registry, List.of(chart)), List.of(chart));
    // The dataset's label sends every name in the index to name, the role's included
    assertTrue(V3SearchFields.fullTextFields(roleIndex).get(ENTITY_NAME).contains("name"));
  }

  /** The V3 test entity names its text field as the shared name label. */
  @Test
  public void testEntityTextFieldFeedsName() {
    Map<String, List<String>> fields =
        V3SearchFields.fullTextFields(List.of(TestEntitySpecBuilder.getSpec()));

    assertTrue(
        fields.get(ENTITY_NAME).stream().anyMatch(source -> source.endsWith(".textFieldOverride")),
        fields.toString());
    assertFalse(fields.get(OTHER).contains("textFieldOverride"), fields.toString());
    assertTrue(
        V3SearchFields.omitsSourceRoot(
            "textFieldOverride", List.of(TestEntitySpecBuilder.getSpec())));
    assertEquals(
        V3SearchFields.analyzerSubfields(List.of(TestEntitySpecBuilder.getSpec())).get(ENTITY_NAME),
        Set.of("text", "stemmed"));
  }

  /**
   * Every field name declared here, for a shared field or for matched fields, is a searchable field
   * name in the bundled models.
   */
  @Test
  public void testCopyToResolvesSharedAndRootFields() {
    List<EntitySpec> dataset = List.of(dataset());

    assertEquals(
        V3SearchFields.copyDestinations(dataset, List.of("name", "entityName", "notAField")),
        List.of(ENTITY_NAME));
    assertEquals(
        V3SearchFields.copyDestinations(dataset, List.of("description", "_search.other")),
        List.of(DESCRIPTION, OTHER));
    assertTrue(V3SearchFields.copyDestinations(dataset, null).isEmpty());
    assertTrue(V3SearchFields.copyDestinations(dataset, List.of(" ")).isEmpty());
  }

  @Test
  public void testDeclaredFieldsNameSearchableFields() {
    Set<String> searchableFieldNames =
        REGISTRY.getEntitySpecs().values().stream()
            .flatMap(entitySpec -> entitySpec.getSearchableFieldSpecs().stream())
            .map(fieldSpec -> fieldSpec.getSearchableAnnotation().getFieldName())
            .collect(Collectors.toSet());
    Set<String> declared =
        Stream.concat(
                V3SearchFields.COLUMN_ARRAY_FIELDS.stream(),
                V3SearchFields.MATCHED_OTHER_FIELDS.stream())
            .collect(Collectors.toSet());

    assertTrue(
        searchableFieldNames.containsAll(declared),
        declared.stream().filter(name -> !searchableFieldNames.contains(name)).toList().toString());
  }

  @Test
  public void testIndexedFieldWeightFollowsTheResolvedScore() {
    List<EntitySpec> specs = List.of(dataset(), REGISTRY.getEntitySpec("businessAttribute"));

    assertEquals(V3SearchFields.weight(specs, "name"), 10f);
    assertEquals(V3SearchFields.weight(specs, "name_1"), 1f);
    assertEquals(V3SearchFields.weight(specs, "qualifiedName"), 10f);
    assertEquals(V3SearchFields.weight(specs, "description"), 1f);
    assertEquals(V3SearchFields.weight(specs, "description_1"), 0.5f);
    assertEquals(V3SearchFields.weight(specs, "tags"), 0.5f);
    assertEquals(V3SearchFields.weight(specs, "tags_1"), 0.25f);
    assertEquals(V3SearchFields.weight(specs, "glossaryTerms"), 0.5f);
    assertEquals(V3SearchFields.weight(specs, "glossaryTerms_1"), 0.25f);
    assertEquals(V3SearchFields.weight(specs, OTHER), 0.5f);
  }

  private static EntitySpec dataset() {
    return REGISTRY.getEntitySpec("dataset");
  }

  private static SearchableFieldSpec fieldSpec(
      FieldType fieldType, String searchLabel, boolean autocomplete) {
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.getFieldName()).thenReturn("title");
    when(annotation.getFieldType()).thenReturn(fieldType);
    when(annotation.getSearchLabel()).thenReturn(Optional.ofNullable(searchLabel));
    when(annotation.indexedFieldName())
        .thenReturn(
            searchLabel == null || searchLabel.isEmpty()
                ? Optional.empty()
                : Optional.of(searchLabel));
    when(annotation.getEntityFieldName()).thenReturn(Optional.empty());
    when(annotation.isV3Searchable()).thenReturn(true);
    when(annotation.resolvedForV3()).thenReturn(annotation);
    when(annotation.isQueryByDefault()).thenReturn(fieldType != FieldType.KEYWORD);
    when(annotation.isEnableAutocomplete()).thenReturn(autocomplete);
    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(annotation);
    return fieldSpec;
  }
}
