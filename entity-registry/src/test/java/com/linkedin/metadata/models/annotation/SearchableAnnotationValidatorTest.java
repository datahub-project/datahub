package com.linkedin.metadata.models.annotation;

import static org.testng.Assert.assertThrows;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
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

    String[] textCompatibleTypes = {
      "KEYWORD", "TEXT", "TEXT_PARTIAL", "WORD_GRAM", "BROWSE_PATH", "URN", "URN_PARTIAL"
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
  public void testSearchTierCrossEntityCompatibility() {
    // Test that searchTier validation works across different entity contexts
    // Since searchTier is used for cross-entity searching, this is important
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Different entities with same searchTier should be compatible
    SearchableAnnotation annotation1 = createAnnotationWithSearchTier("TEXT", 1, Optional.empty());
    SearchableAnnotation annotation2 =
        createAnnotationWithSearchTier("KEYWORD", 1, Optional.empty());

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "entity1.field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "entity2.field1"));

    // Should not throw exception - same searchTier across entities is valid
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  @Test
  public void testSearchTierDifferentValuesAcrossEntities() {
    // Test that different searchTier values don't cause conflicts
    // This is important since searchTier is used for cross-entity searching
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Different searchTier values should not conflict
    SearchableAnnotation annotation1 = createAnnotationWithSearchTier("TEXT", 1, Optional.empty());
    SearchableAnnotation annotation2 =
        createAnnotationWithSearchTier("KEYWORD", 2, Optional.empty());

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "entity1.field1"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "entity2.field1"));

    // Should not throw exception - different searchTier values are fine
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
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
  public void testSearchTierWithLabels() {
    // Test that searchTier works correctly with searchLabel and searchLabel
    List<SearchableAnnotationValidator.AnnotatedField> fields = new ArrayList<>();

    // Fields with searchTier and labels should be compatible
    SearchableAnnotation annotation1 =
        createAnnotationWithSearchTier("TEXT", 1, Optional.of("name_sort"));
    SearchableAnnotation annotation2 =
        createAnnotationWithSearchTier("KEYWORD", 1, Optional.of("name_sort"));

    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation1, DataSchema.Type.STRING, "entity1.name"));
    fields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation2, DataSchema.Type.STRING, "entity2.name"));

    // Should not throw exception - compatible types and same labels
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(fields);
  }

  private SearchableAnnotation createAnnotation(String fieldType, Optional<String> searchLabel) {
    return new SearchableAnnotation(
        "testField",
        SearchableAnnotation.FieldType.valueOf(fieldType),
        true, // queryByDefault
        false, // enableAutocomplete
        false, // addToFilters
        false, // addHasValuesToFilters
        Optional.empty(), // filterNameOverride
        Optional.empty(), // hasValuesFilterNameOverride
        1.0, // boostScore
        Optional.empty(), // hasValuesFieldName
        Optional.empty(), // numValuesFieldName
        java.util.Map.of(), // weightsPerFieldValue
        java.util.List.of(), // fieldNameAliases
        false, // includeQueryEmptyAggregation
        false, // includeSystemModifiedAt
        Optional.empty(), // systemModifiedAtFieldName
        Optional.empty(), // searchTier
        searchLabel, // searchLabel
        Optional.empty(), // searchIndexed
        Optional.empty(), // entityFieldName
        Optional.empty(), // eagerGlobalOrdinals
        false);
  }

  private SearchableAnnotation createAnnotationWithSearchTier(
      String fieldType, int searchTier, Optional<String> searchLabel) {
    return new SearchableAnnotation(
        "testField",
        SearchableAnnotation.FieldType.valueOf(fieldType),
        true, // queryByDefault
        false, // enableAutocomplete
        false, // addToFilters
        false, // addHasValuesToFilters
        Optional.empty(), // filterNameOverride
        Optional.empty(), // hasValuesFilterNameOverride
        1.0, // boostScore
        Optional.empty(), // hasValuesFieldName
        Optional.empty(), // numValuesFieldName
        java.util.Map.of(), // weightsPerFieldValue
        java.util.List.of(), // fieldNameAliases
        false, // includeQueryEmptyAggregation
        false, // includeSystemModifiedAt
        Optional.empty(), // systemModifiedAtFieldName
        Optional.of(searchTier), // searchTier
        searchLabel, // searchLabel
        Optional.empty(), // searchIndexed
        Optional.empty(), // entityFieldName
        Optional.empty(), // eagerGlobalOrdinals
        false);
  }
}
