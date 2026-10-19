package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.COLUMNS;
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

    assertEquals(fields.keySet(), Set.of(ENTITY_NAME, QUALIFIED_NAME, DESCRIPTION, COLUMNS, OTHER));
    // Matched fields follow this order, so the catch-all comes last
    assertEquals(List.copyOf(fields.keySet()).get(fields.size() - 1), OTHER);
    assertTrue(
        fields.get(ENTITY_NAME).containsAll(List.of("name", "editedName")), fields.toString());
    assertEquals(fields.get(QUALIFIED_NAME), List.of("qualifiedName"));
    assertTrue(
        fields
            .get(DESCRIPTION)
            .containsAll(
                List.of(
                    "description",
                    "editedDescription",
                    "fieldDescriptions",
                    "editedFieldDescriptions")),
        fields.toString());
    assertTrue(
        fields.get(COLUMNS).containsAll(List.of("fieldPaths", "fieldLabels")), fields.toString());
    assertFalse(fields.containsKey("tags"), fields.toString());
    assertFalse(fields.containsKey("glossaryTerms"), fields.toString());
    assertTrue(fields.get(OTHER).contains("platform"), fields.toString());
    assertFalse(fields.get(OTHER).contains("tags"), fields.toString());
    // A field that feeds a named shared field does not also land in other
    assertTrue(
        fields.get(OTHER).stream()
            .noneMatch(field -> List.of("name", "description", "fieldPaths").contains(field)),
        fields.toString());
  }

  /** A dataset search reads the columns' arrays, but matched fields never fetch them. */
  @Test
  public void testMatchedFieldSourcesLeaveOutTheColumnArrays() {
    Map<String, List<String>> fields = V3SearchFields.fullTextFields(List.of(dataset()));
    assertTrue(
        fields.get(COLUMNS).containsAll(List.of("fieldPaths", "fieldLabels")), fields.toString());

    List<String> sources = V3SearchFields.matchedFieldSources(fields);

    assertTrue(
        sources.containsAll(List.of("name", "qualifiedName", "description")), sources.toString());
    assertTrue(sources.stream().noneMatch(COLUMN_ARRAYS::contains), sources.toString());
    // Nor wherever a label sends one
    assertEquals(
        V3SearchFields.matchedFieldSources(
            Map.of(DESCRIPTION, List.of("description", "fieldDescriptions"))),
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
    assertTrue(
        datasetSources.containsAll(List.of("name", "editedName")), datasetSources.toString());
    assertFalse(datasetSources.contains("tags"), datasetSources.toString());
    assertFalse(datasetSources.contains("glossaryTerms"), datasetSources.toString());
    assertTrue(
        datasetSources.stream()
            .noneMatch(List.of("customProperties", "platform", "domains")::contains),
        datasetSources.toString());

    Map<String, List<String>> documentFields =
        V3SearchFields.fullTextFields(List.of(REGISTRY.getEntitySpec("document")));
    List<String> freeText = List.of("text", "semanticText", "customProperties");
    assertTrue(documentFields.get(OTHER).containsAll(freeText), documentFields.toString());
    List<String> documentSources = V3SearchFields.matchedFieldSources(documentFields);
    assertTrue(documentSources.contains("title"), documentSources.toString());
    assertTrue(documentSources.stream().noneMatch(freeText::contains), documentSources.toString());
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

  /** The service entity labels no name, so the field its _entityName alias names is the name. */
  @Test
  public void testEntityWithoutNameLabelSendsItsEntityNameAliasToEntityName() {
    EntitySpec service = REGISTRY.getEntitySpec("service");

    Map<String, List<String>> fields = V3SearchFields.fullTextFields(List.of(service));

    assertEquals(fields.get(ENTITY_NAME), List.of("displayName"));
    assertFalse(fields.get(OTHER).contains("displayName"), fields.toString());
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
            "normalizer",
            "keyword_normalizer",
            "ignore_above",
            8191,
            "fields",
            Map.of(
                "text",
                text,
                "stemmed",
                stemmed,
                "keyword",
                Map.of("type", "keyword", "ignore_above", 8191)));
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
        V3SearchFields.mapping("origin", false),
        Map.of("type", "keyword", "normalizer", "keyword_normalizer", "ignore_above", 8191));
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

    assertTrue(fields.get(ENTITY_NAME).contains("textFieldOverride"), fields.toString());
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

  private static EntitySpec dataset() {
    return REGISTRY.getEntitySpec("dataset");
  }

  private static SearchableFieldSpec fieldSpec(
      FieldType fieldType, String searchLabel, boolean autocomplete) {
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.getFieldName()).thenReturn("title");
    when(annotation.getFieldType()).thenReturn(fieldType);
    when(annotation.getSearchLabel()).thenReturn(Optional.ofNullable(searchLabel));
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
