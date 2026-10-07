package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.AUTOCOMPLETE;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.COLUMNS;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.DESCRIPTION;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.ENTITY_NAME;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.OTHER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.QUALIFIED_NAME;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.STRUCTURED_PROPERTIES;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
  private static final Map<String, String> PARTIAL_NGRAM_CONFIG =
      Map.of("type", "search_as_you_type", "max_shingle_size", "4");
  private static final List<String> COLUMN_ARRAYS =
      List.of(
          "fieldPaths",
          "fieldLabels",
          "fieldDescriptions",
          "editedFieldDescriptions",
          "fieldTags",
          "editedFieldTags",
          "fieldGlossaryTerms",
          "editedFieldGlossaryTerms");

  @Test
  public void testDatasetFullTextFieldsAndTheirSources() {
    Map<String, List<String>> fields = V3SearchFields.fullTextFields(List.of(dataset()));

    assertEquals(
        fields.keySet(),
        Set.of(ENTITY_NAME, QUALIFIED_NAME, DESCRIPTION, COLUMNS, STRUCTURED_PROPERTIES, OTHER));
    // Structured property values copy in from the properties' own fields, not from root fields
    assertEquals(fields.get(STRUCTURED_PROPERTIES), List.of());
    // Matched fields follow this order, so the catch-all comes last
    assertEquals(List.copyOf(fields.keySet()).get(fields.size() - 1), OTHER);
    assertTrue(fields.get(ENTITY_NAME).contains("name"), fields.toString());
    // The sortable name holds one value per entity, so an edited name is searched in other
    assertFalse(fields.get(ENTITY_NAME).contains("editedName"), fields.toString());
    assertTrue(fields.get(OTHER).contains("editedName"), fields.toString());
    assertEquals(fields.get(QUALIFIED_NAME), List.of("qualifiedName"));
    assertTrue(
        fields.get(DESCRIPTION).containsAll(List.of("description", "editedDescription")),
        fields.toString());
    assertTrue(
        fields
            .get(COLUMNS)
            .containsAll(
                List.of(
                    "fieldPaths", "fieldDescriptions", "editedFieldDescriptions", "fieldLabels")),
        fields.toString());
    assertTrue(fields.get(OTHER).containsAll(List.of("tags", "platform")), fields.toString());
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
    assertTrue(fields.get(COLUMNS).containsAll(COLUMN_ARRAYS), fields.toString());

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
   * Of other, matched fields fetch only what names or tags the entity: a document's text, custom
   * properties and other lists of urns are searched but could be any size.
   */
  @Test
  public void testMatchedFieldSourcesKeepOnlyNamesAndTagsOfOther() {
    List<String> datasetSources =
        V3SearchFields.matchedFieldSources(V3SearchFields.fullTextFields(List.of(dataset())));
    assertTrue(
        datasetSources.containsAll(List.of("name", "editedName", "tags", "glossaryTerms")),
        datasetSources.toString());
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
    SearchableFieldSpec labeled = fieldSpec(FieldType.TEXT, "entityName", false);
    SearchableFieldSpec unlabeled = fieldSpec(FieldType.TEXT, null, false);

    // Two sources of one root field: the root copies into the label only
    assertEquals(
        V3SearchFields.destinations(List.of(labeled, unlabeled), Set.of()), Set.of(ENTITY_NAME));
    assertEquals(V3SearchFields.destinations(List.of(unlabeled), Set.of()), Set.of(OTHER));
    assertEquals(
        V3SearchFields.destinations(List.of(fieldSpec(FieldType.TEXT, null, true)), Set.of()),
        Set.of(OTHER, AUTOCOMPLETE));
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
    assertEquals(V3SearchFields.autocompleteFields(List.of(service)).get(0), "displayName");
  }

  @Test
  public void testAutocompleteFieldsPutTheNameFirst() {
    List<String> fields = V3SearchFields.autocompleteFields(List.of(dataset()));

    // The key fields come first in the dataset's aspects; the name still leads
    assertEquals(fields.get(0), "name");
    assertTrue(fields.containsAll(List.of("qualifiedName", "id", "platform")), fields.toString());
  }

  @Test
  public void testMappingShapes() {
    Map<String, Object> text =
        Map.of("type", "text", "analyzer", "v3_text", "search_analyzer", "v3_text_search");
    Map<String, Object> stemmed =
        Map.of("type", "text", "analyzer", "v3_stemmed", "search_analyzer", "v3_stemmed_search");

    // A name keeps a normalized keyword for exact match and sorting, with the stored casing in
    // .keyword
    assertEquals(
        V3SearchFields.mapping(ENTITY_NAME, true, PARTIAL_NGRAM_CONFIG),
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
                Map.of("type", "keyword", "ignore_above", 8191))));
    // Other full-text fields only hold their subfields
    assertEquals(
        V3SearchFields.mapping(DESCRIPTION, true, PARTIAL_NGRAM_CONFIG),
        Map.of(
            "type",
            "keyword",
            "index",
            false,
            "doc_values",
            false,
            "fields",
            Map.of("text", text, "stemmed", stemmed)));
    assertEquals(
        V3SearchFields.mapping(AUTOCOMPLETE, true, PARTIAL_NGRAM_CONFIG),
        Map.of(
            "type",
            "keyword",
            "normalizer",
            "keyword_normalizer",
            "ignore_above",
            8191,
            "doc_values",
            false,
            "fields",
            Map.of(
                "ngram",
                Map.of(
                    "type",
                    "search_as_you_type",
                    "max_shingle_size",
                    "4",
                    "analyzer",
                    "partial"))));
    // A label a model names stays an indexed keyword for its sorts and filters, analyzed too when
    // a field queried by default feeds it
    assertEquals(
        V3SearchFields.mapping("owner", true, PARTIAL_NGRAM_CONFIG),
        Map.of(
            "type",
            "keyword",
            "normalizer",
            "keyword_normalizer",
            "ignore_above",
            8191,
            "fields",
            Map.of("text", text, "stemmed", stemmed)));
    // A label no full-text field feeds stays a plain keyword
    assertEquals(
        V3SearchFields.mapping("origin", false, PARTIAL_NGRAM_CONFIG),
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
    // The dataset's label sends every name in the index to entityName, the role's included
    assertTrue(V3SearchFields.fullTextFields(roleIndex).get(ENTITY_NAME).contains("name"));
  }

  /**
   * Every field name declared here, for a shared field or for matched fields, is a searchable field
   * name in the bundled models.
   */
  @Test
  public void testDeclaredFieldsNameSearchableFields() {
    Set<String> searchableFieldNames =
        REGISTRY.getEntitySpecs().values().stream()
            .flatMap(entitySpec -> entitySpec.getSearchableFieldSpecs().stream())
            .map(fieldSpec -> fieldSpec.getSearchableAnnotation().getFieldName())
            .collect(Collectors.toSet());
    Set<String> declared =
        Stream.concat(
                V3SearchFields.DECLARED_FIELDS.keySet().stream(),
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
    when(annotation.isQueryByDefault()).thenReturn(fieldType != FieldType.KEYWORD);
    when(annotation.isEnableAutocomplete()).thenReturn(autocomplete);
    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(annotation);
    return fieldSpec;
  }
}
