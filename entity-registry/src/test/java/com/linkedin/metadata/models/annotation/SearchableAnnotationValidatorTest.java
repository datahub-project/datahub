package com.linkedin.metadata.models.annotation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.Test;

/** Tests for {@link SearchableAnnotationValidator} */
public class SearchableAnnotationValidatorTest {

  @Test
  public void testValidSearchLabelCompatibility() {
    // Create compatible annotations with same searchLabel
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Both string fields with TEXT-compatible fieldTypes
    SearchableAnnotation annotation1 = createAnnotation("TEXT", Optional.of("name_sort"));
    SearchableAnnotation annotation2 = createAnnotation("KEYWORD", Optional.of("name_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "field2"));

    // Should not throw exception
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testValidSearchLabelCompatibility2() {
    // Create compatible annotations with same searchLabel
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Both string fields with TEXT-compatible fieldTypes
    SearchableAnnotation annotation1 = createAnnotation("TEXT_PARTIAL", Optional.of("relevance"));
    SearchableAnnotation annotation2 = createAnnotation("URN", Optional.of("relevance"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "field2"));

    // Should not throw exception
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testSharedLabelRequiresTheSameHasValuesFieldName() {
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("TEXT", Optional.of("description"), Optional.of("hasDescription")),
            DataSchema.Type.STRING,
            "dataset.description"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("TEXT", Optional.of("description"), Optional.of("hasDescription")),
            DataSchema.Type.STRING,
            "editable.description"));

    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testSharedLabelRejectsAMissingHasValuesFieldName() {
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("TEXT", Optional.of("description"), Optional.of("hasDescription")),
            DataSchema.Type.STRING,
            "dataset.description"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("TEXT", Optional.of("description")),
            DataSchema.Type.STRING,
            "editable.description"));

    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testSharedLabelRejectsADifferentHasValuesFieldName() {
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("TEXT", Optional.of("description"), Optional.of("hasDescription")),
            DataSchema.Type.STRING,
            "dataset.description"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation(
                "TEXT", Optional.of("description"), Optional.of("hasAssertionDescription")),
            DataSchema.Type.STRING,
            "assertion.description"));

    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testIncompatiblePDLTypes() {
    // Create annotations with same searchLabel but different PDL types
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    SearchableAnnotation annotation1 = createAnnotation("TEXT", Optional.of("name_sort"));
    SearchableAnnotation annotation2 = createAnnotation("TEXT", Optional.of("name_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.INT, "field2"));

    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testIncompatibleFieldTypes() {
    // Create annotations with same searchLabel but incompatible fieldTypes
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    SearchableAnnotation annotation1 = createAnnotation("TEXT", Optional.of("name_sort"));
    SearchableAnnotation annotation2 = createAnnotation("BOOLEAN", Optional.of("name_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "field2"));

    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testObjectFieldTypeIncompatibility() {
    // OBJECT fieldType should not be compatible with anything else
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    SearchableAnnotation annotation1 = createAnnotation("OBJECT", Optional.of("name_sort"));
    SearchableAnnotation annotation2 = createAnnotation("TEXT", Optional.of("name_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "field2"));

    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testExactMatchTypesRequireIdentical() {
    // BOOLEAN, COUNT, DATETIME, DOUBLE must be identical
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    SearchableAnnotation annotation1 = createAnnotation("BOOLEAN", Optional.of("status_sort"));
    SearchableAnnotation annotation2 = createAnnotation("COUNT", Optional.of("status_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.BOOLEAN, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.BOOLEAN, "field2"));

    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testValidExactMatchTypes() {
    // Same exact match types should be compatible
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    SearchableAnnotation annotation1 = createAnnotation("BOOLEAN", Optional.of("status_sort"));
    SearchableAnnotation annotation2 = createAnnotation("BOOLEAN", Optional.of("status_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.BOOLEAN, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.BOOLEAN, "field2"));

    // Should not throw exception
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testTextCompatibleTypes() {
    // Test all text-compatible types together
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // String field types resolve to one indexed keyword root. Browse path v1 is not a Search V3
    // type, so a V3 annotation cannot use it.
    String[] textCompatibleTypes = {
      "KEYWORD", "TEXT", "TEXT_PARTIAL", "WORD_GRAM", "URN", "URN_PARTIAL"
    };

    for (int i = 0; i < textCompatibleTypes.length; i++) {
      SearchableAnnotation annotation =
          createAnnotation(textCompatibleTypes[i], Optional.of("text_sort"));
      fields.add(
          new SearchableAnnotationValidator.AnnotatedField(
              annotation, DataSchema.Type.STRING, "field" + i));
    }

    // Should not throw exception
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);

    assertThrows(
        ModelValidationException.class,
        () -> createAnnotation("BROWSE_PATH", Optional.of("text_sort")));
  }

  @Test
  public void testSingleFieldNoValidation() {
    // Single field should not require validation
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    SearchableAnnotation annotation = createAnnotation("TEXT", Optional.of("name_sort"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation, DataSchema.Type.STRING, "field1"));

    // Should not throw exception
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testMixedLabelsValidation() {
    // Test fields with different combinations of searchLabel and searchLabel
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Same searchLabel, different searchLabel - should validate searchLabel compatibility
    SearchableAnnotation annotation1 = createAnnotation("TEXT", Optional.of("name_sort"));
    SearchableAnnotation annotation2 = createAnnotation("KEYWORD", Optional.of("name_sort"));

    // Same searchLabel, different searchLabel - should validate searchLabel compatibility
    SearchableAnnotation annotation3 = createAnnotation("TEXT_PARTIAL", Optional.of("title_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "field2"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation3, DataSchema.Type.STRING, "field3"));

    // Should not throw exception
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testRankingLabelIncompatibility() {
    // Test incompatible searchLabel fields
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    SearchableAnnotation annotation1 = createAnnotation("TEXT", Optional.of("relevance"));
    SearchableAnnotation annotation2 = createAnnotation("COUNT", Optional.of("relevance"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.INT, "field2"));

    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testSortLabelAcrossDifferentEntities() {
    // Test that searchLabel works across different entities
    // Since searchLabel copies values to common fields, this is critical
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Same searchLabel across different entities should be compatible
    SearchableAnnotation annotation1 = createAnnotation("TEXT", Optional.of("name_sort"));
    SearchableAnnotation annotation2 = createAnnotation("KEYWORD", Optional.of("name_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "entity1.name"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "entity2.name"));

    // Should not throw exception - same searchLabel across entities is valid
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testRankingLabelAcrossDifferentEntities() {
    // Test that searchLabel works across different entities
    // Since searchLabel copies values to common fields, this is critical
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Same searchLabel across different entities should be compatible
    SearchableAnnotation annotation1 = createAnnotation("TEXT_PARTIAL", Optional.of("relevance"));
    SearchableAnnotation annotation2 = createAnnotation("URN", Optional.of("relevance"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "entity1.title"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "entity2.urn"));

    // Should not throw exception - same searchLabel across entities is valid
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testEmptyLabelsNoValidation() {
    // Test that empty/absent labels don't trigger validation
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Fields without labels should not require validation
    SearchableAnnotation annotation1 = createAnnotation("TEXT", Optional.empty());
    SearchableAnnotation annotation2 = createAnnotation("KEYWORD", Optional.empty());

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "field2"));

    // Should not throw exception - no labels to validate
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testMixedLabelScenarios() {
    // Test complex scenarios with mixed labels across entities
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Entity 1: has both searchLabel and searchLabel
    SearchableAnnotation annotation1 = createAnnotation("TEXT", Optional.of("name_sort"));

    // Entity 2: has same searchLabel but different searchLabel
    SearchableAnnotation annotation2 = createAnnotation("KEYWORD", Optional.of("name_sort"));

    // Entity 3: has same searchLabel but different searchLabel
    SearchableAnnotation annotation3 = createAnnotation("TEXT_PARTIAL", Optional.of("title_sort"));

    // Entity 4: has no labels
    SearchableAnnotation annotation4 = createAnnotation("URN", Optional.empty());

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "entity1.name"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "entity2.name"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation3, DataSchema.Type.STRING, "entity3.title"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation4, DataSchema.Type.STRING, "entity4.urn"));

    // Should not throw exception - all labels are compatible
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testIncompatibleLabelsAcrossEntities() {
    // Test that incompatible labels across entities are caught
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Same searchLabel but incompatible field types across entities
    SearchableAnnotation annotation1 = createAnnotation("TEXT", Optional.of("status_sort"));
    SearchableAnnotation annotation2 = createAnnotation("BOOLEAN", Optional.of("status_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "entity1.status"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.BOOLEAN, "entity2.status"));

    // Should throw exception - incompatible field types for same searchLabel
    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testSameSearchLabelRequiresTheSameBoost() {
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("WORD_GRAM", Optional.of("entityName"), 10.0),
            DataSchema.Type.STRING,
            "dataset.datasetProperties.name"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("WORD_GRAM", Optional.of("entityName"), 8.0),
            DataSchema.Type.STRING,
            "mlFeature.mlFeatureKey.name"));

    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testSemanticBucketsShareOneBoost() {
    List<SearchableAnnotationValidator.AnnotatedField> names = new ArrayList<>();
    names.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("WORD_GRAM", Optional.of("entityName"), 10.0),
            DataSchema.Type.STRING,
            "dataset.datasetProperties.name"));
    names.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("TEXT_PARTIAL", Optional.of("entityName"), 10.0),
            DataSchema.Type.STRING,
            "dataset.editableDatasetProperties.editedName"));
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(names);

    List<SearchableAnnotationValidator.AnnotatedField> descriptions = new ArrayList<>();
    descriptions.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("TEXT", Optional.of("description"), 1.0),
            DataSchema.Type.STRING,
            "dataset.datasetProperties.description"));
    descriptions.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("TEXT", Optional.of("description"), 1.0),
            DataSchema.Type.STRING,
            "schema.schemaField.fieldDescriptions"));
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(descriptions);

    List<SearchableAnnotationValidator.AnnotatedField> tags = new ArrayList<>();
    tags.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("URN", Optional.of("tags"), 0.5),
            DataSchema.Type.STRING,
            "dataset.globalTags.tags"));
    tags.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("URN", Optional.of("tags"), 0.5),
            DataSchema.Type.STRING,
            "schema.schemaField.fieldTags"));
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(tags);

    List<SearchableAnnotationValidator.AnnotatedField> created = new ArrayList<>();
    created.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("DATETIME", Optional.of("createdAt"), 1.0),
            DataSchema.Type.LONG,
            "dataset.datasetProperties.created"));
    created.add(
        new SearchableAnnotationValidator.AnnotatedField(
            createAnnotation("DATETIME", Optional.of("createdAt"), 1.0),
            DataSchema.Type.LONG,
            "chart.chartInfo.lastModified"));
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(created);
  }

  @Test
  public void testV3LabelsShareOneBoost() {
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            v3Annotation("name", 10.0), DataSchema.Type.STRING, "dataset.name"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            v3Annotation("name", 10.0), DataSchema.Type.STRING, "chart.title"));
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);

    List<SearchableAnnotationValidator.AnnotatedField> conflicting = new ArrayList<>();
    conflicting.add(
        new SearchableAnnotationValidator.AnnotatedField(
            v3Annotation("name", 10.0), DataSchema.Type.STRING, "dataset.name"));
    conflicting.add(
        new SearchableAnnotationValidator.AnnotatedField(
            v3Annotation("name", 1.0), DataSchema.Type.STRING, "chart.title"));
    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(conflicting));
  }

  @Test
  public void testFallbackUnionAddsNgram() {
    SearchableAnnotation fallback =
        fromMap(annotation("TEXT", "name", null), DataSchema.Type.STRING);
    DataList mappings = new DataList();
    mappings.add(entry(null, "KEYWORD", null, null));
    mappings.add(entry("text", "TEXT", null, null));
    mappings.add(entry("stemmed", "STEMMED", null, null));
    mappings.add(entry("ngram", "TEXT_PARTIAL", null, null));
    SearchableAnnotation extra =
        fromMap(annotation("TEXT", "name", mappings), DataSchema.Type.STRING);

    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(fallback, DataSchema.Type.STRING, "a"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(extra, DataSchema.Type.STRING, "b"));
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);

    FieldMapping.Resolved resolved =
        FieldMapping.union("name", List.of(fallback.getV3Mappings(), extra.getV3Mappings()));
    assertEquals(
        resolved.getSubfields().stream()
            .map(FieldMapping::getSubfield)
            .collect(java.util.stream.Collectors.toSet()),
        java.util.Set.of("text", "stemmed", "ngram"));
  }

  @Test
  public void testSecondRootIsRejected() {
    DataList mappings = new DataList();
    mappings.add(entry(null, "KEYWORD", null, null));
    mappings.add(entry(null, "DATETIME", null, null));
    assertThrows(
        ModelValidationException.class,
        () -> fromMap(annotation("TEXT", "name", mappings), DataSchema.Type.STRING));
  }

  @Test
  public void testOmittedIndexStillMeansIndexed() {
    SearchableAnnotation indexed =
        fromMap(annotation("TEXT", "description", null), DataSchema.Type.STRING);
    DataList mappings = new DataList();
    mappings.add(entry(null, "KEYWORD", false, false));
    mappings.add(entry("text", "TEXT", null, null));
    mappings.add(entry("stemmed", "STEMMED", null, null));
    SearchableAnnotation unindexed =
        fromMap(annotation("TEXT", "description", mappings), DataSchema.Type.STRING);
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(indexed, DataSchema.Type.STRING, "a"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(unindexed, DataSchema.Type.STRING, "b"));
    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testConflictingSubfieldType() {
    DataList text = new DataList();
    text.add(entry(null, "KEYWORD", null, null));
    text.add(entry("text", "TEXT", null, null));
    DataList urn = new DataList();
    urn.add(entry(null, "KEYWORD", null, null));
    urn.add(entry("text", "URN", null, null));
    SearchableAnnotation first = fromMap(annotation("TEXT", "name", text), DataSchema.Type.STRING);
    SearchableAnnotation second = fromMap(annotation("TEXT", "name", urn), DataSchema.Type.STRING);
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(first, DataSchema.Type.STRING, "a"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(second, DataSchema.Type.STRING, "b"));
    assertThrows(
        ModelValidationException.class,
        () -> SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields));
  }

  @Test
  public void testSubfieldWithoutLabel() {
    DataList mappings = new DataList();
    mappings.add(entry(null, "KEYWORD", null, null));
    mappings.add(entry("text", "TEXT", null, null));
    assertThrows(
        ModelValidationException.class,
        () -> fromMap(annotation("TEXT", null, mappings), DataSchema.Type.STRING));
  }

  @Test
  public void testStringTypeOnNumericLabel() {
    DataList mappings = new DataList();
    mappings.add(entry(null, "DATETIME", null, null));
    mappings.add(entry("text", "TEXT", null, null));
    assertThrows(
        ModelValidationException.class,
        () -> fromMap(annotation("DATETIME", "sourceCreatedAt", mappings), DataSchema.Type.LONG));
  }

  private static DataMap entry(String subfield, String type, Boolean index, Boolean docValues) {
    DataMap entry = new DataMap();
    if (subfield != null) {
      entry.put("subfield", subfield);
    }
    entry.put("type", type);
    if (index != null) {
      entry.put("index", index);
    }
    if (docValues != null) {
      entry.put("docValues", docValues);
    }
    return entry;
  }

  private static DataMap annotation(String fieldType, String searchLabel, DataList mappings) {
    DataMap annotation = new DataMap();
    annotation.put("fieldType", fieldType);
    DataMap v3 = new DataMap();
    if (searchLabel != null) {
      v3.put("searchLabel", searchLabel);
    }
    if (mappings != null) {
      v3.put("mappings", mappings);
    }
    annotation.put("v3", v3);
    return annotation;
  }

  private static SearchableAnnotation fromMap(DataMap annotation, DataSchema.Type schemaType) {
    return SearchableAnnotation.fromPegasusAnnotationObject(
        annotation, "field", schemaType, "test");
  }

  private SearchableAnnotation v3Annotation(String searchLabel, double boostScore) {
    DataMap overlay = new DataMap();
    overlay.put("searchLabel", searchLabel);
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    annotationMap.put("boostScore", boostScore);
    annotationMap.put("v3", overlay);
    return SearchableAnnotation.fromPegasusAnnotationObject(
        annotationMap, "field", DataSchema.Type.STRING, "test");
  }

  private SearchableAnnotation createAnnotation(String fieldType, Optional<String> searchLabel) {
    return createAnnotation(fieldType, searchLabel, Optional.empty(), 1.0);
  }

  private SearchableAnnotation createAnnotation(
      String fieldType, Optional<String> searchLabel, double boostScore) {
    return createAnnotation(fieldType, searchLabel, Optional.empty(), boostScore);
  }

  private SearchableAnnotation createAnnotation(
      String fieldType, Optional<String> searchLabel, Optional<String> hasValuesFieldName) {
    return createAnnotation(fieldType, searchLabel, hasValuesFieldName, 1.0);
  }

  private SearchableAnnotation createAnnotation(
      String fieldType,
      Optional<String> searchLabel,
      Optional<String> hasValuesFieldName,
      double boostScore) {
    return new SearchableAnnotation(
        "testField",
        SearchableAnnotation.FieldType.valueOf(fieldType),
        true, // queryByDefault
        false, // enableAutocomplete
        false, // addToFilters
        false, // addHasValuesToFilters
        Optional.empty(), // filterNameOverride
        Optional.empty(), // hasValuesFilterNameOverride
        boostScore, // boostScore
        hasValuesFieldName,
        Optional.empty(), // numValuesFieldName
        java.util.Map.of(), // weightsPerFieldValue
        java.util.List.of(), // fieldNameAliases
        false, // includeQueryEmptyAggregation
        false, // includeSystemModifiedAt
        Optional.empty(), // systemModifiedAtFieldName
        searchLabel, // searchLabel
        Optional.empty(), // searchIndexed
        Optional.empty(), // entityFieldName
        Optional.empty(), // eagerGlobalOrdinals
        false);
  }
}
