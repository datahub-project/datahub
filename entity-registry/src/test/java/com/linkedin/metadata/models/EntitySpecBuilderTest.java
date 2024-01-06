package com.linkedin.metadata.models;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.test.SearchFeatures;
import com.datahub.test.Snapshot;
import com.datahub.test.TestBrowsePaths;
import com.datahub.test.TestEntityInfo;
import com.datahub.test.TestEntityKey;
import com.datahub.test.invalid.DuplicateSearchableFields;
import com.datahub.test.invalid.InvalidSearchableFieldType;
import com.datahub.test.invalid.MissingAspectAnnotation;
import com.datahub.test.invalid.MissingRelationshipName;
import com.datahub.test.invalid.NonNumericSearchScoreField;
import com.datahub.test.invalid.NonSingularSearchScoreField;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

/** Tests the capabilities of {@link EntitySpecBuilder} */
public class EntitySpecBuilderTest {

  @Test
  public void testBuildAspectSpecValidationAspectMissingAnnotation() {
    assertThrows(
        ModelValidationException.class,
        () ->
            new EntitySpecBuilder()
                .buildAspectSpec(new MissingAspectAnnotation().schema(), RecordTemplate.class));
  }

  @Test
  public void testBuildAspectSpecValidationInvalidSearchableFieldType() {
    assertThrows(
        ModelValidationException.class,
        () ->
            new EntitySpecBuilder()
                .buildAspectSpec(new InvalidSearchableFieldType().schema(), RecordTemplate.class));
  }

  @Test
  public void testBuildAspectSpecValidationDuplicateSearchableFields() {
    AspectSpec aspectSpec =
        new EntitySpecBuilder()
            .buildAspectSpec(new DuplicateSearchableFields().schema(), RecordTemplate.class);

    aspectSpec
        .getSearchableFieldSpecs()
        .forEach(
            searchableFieldSpec -> {
              String name = searchableFieldSpec.getSearchableAnnotation().getFieldName();
              assertTrue("textField".equals(name) || "textField2".equals(name));
            });
  }

  @Test
  public void testBuildAspectSpecValidationMissingRelationshipName() {
    assertThrows(
        ModelValidationException.class,
        () ->
            new EntitySpecBuilder()
                .buildAspectSpec(new MissingRelationshipName().schema(), RecordTemplate.class));
  }

  @Test
  public void testBuildAspectSpecValidationNonNumericSearchScoreField() {
    assertThrows(
        ModelValidationException.class,
        () ->
            new EntitySpecBuilder()
                .buildAspectSpec(new NonNumericSearchScoreField().schema(), RecordTemplate.class));
  }

  @Test
  public void testBuildAspectSpecValidationNonSingularSearchScoreField() {
    assertThrows(
        ModelValidationException.class,
        () ->
            new EntitySpecBuilder()
                .buildAspectSpec(new NonSingularSearchScoreField().schema(), RecordTemplate.class));
  }

  @Test
  public void testBuildEntitySpecs() {

    // Instantiate the test Snapshot
    final Snapshot snapshot = new Snapshot();
    final List<EntitySpec> validEntitySpecs =
        new EntitySpecBuilder().buildEntitySpecs(snapshot.schema());

    // Assert single entity.
    assertEquals(1, validEntitySpecs.size());

    // Assert on Entity Spec
    final EntitySpec testEntitySpec = validEntitySpecs.get(0);
    assertEquals("testEntity", testEntitySpec.getName());

    // Assert on Aspect Specs
    final Map<String, AspectSpec> aspectSpecMap = testEntitySpec.getAspectSpecMap();
    assertEquals(5, aspectSpecMap.size());
    assertTrue(aspectSpecMap.containsKey("testEntityKey"));
    assertTrue(aspectSpecMap.containsKey("testBrowsePaths"));
    assertTrue(aspectSpecMap.containsKey("testEntityInfo"));
    assertTrue(aspectSpecMap.containsKey("searchFeatures"));

    // Assert on TestEntityKey
    validateTestEntityKey(aspectSpecMap.get("testEntityKey"));

    // Assert on BrowsePaths Aspect
    validateBrowsePaths(aspectSpecMap.get("testBrowsePaths"));

    // Assert on TestEntityInfo Aspect
    validateTestEntityInfo(aspectSpecMap.get("testEntityInfo"));

    // Assert on SearchFeatures Aspect
    validateSearchFeatures(aspectSpecMap.get("searchFeatures"));
  }

  private void validateTestEntityKey(final AspectSpec keyAspectSpec) {
    assertEquals("testEntityKey", keyAspectSpec.getName());
    assertEquals(
        new TestEntityKey().schema().getFullName(), keyAspectSpec.getPegasusSchema().getFullName());

    // Assert on Searchable Fields
    assertEquals(2, keyAspectSpec.getSearchableFieldSpecs().size()); // keyPart1, keyPart3
    assertEquals(
        "keyPart1",
        keyAspectSpec
            .getSearchableFieldSpecMap()
            .get(new PathSpec("keyPart1").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.TEXT,
        keyAspectSpec
            .getSearchableFieldSpecMap()
            .get(new PathSpec("keyPart1").toString())
            .getSearchableAnnotation()
            .getFieldType());
    assertEquals(
        "keyPart3",
        keyAspectSpec
            .getSearchableFieldSpecMap()
            .get(new PathSpec("keyPart3").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.KEYWORD,
        keyAspectSpec
            .getSearchableFieldSpecMap()
            .get(new PathSpec("keyPart3").toString())
            .getSearchableAnnotation()
            .getFieldType());

    // Assert on Relationship Field
    assertEquals(1, keyAspectSpec.getRelationshipFieldSpecs().size());
    assertEquals(
        "keyForeignKey",
        keyAspectSpec
            .getRelationshipFieldSpecMap()
            .get(new PathSpec("keyPart2").toString())
            .getRelationshipName());
  }

  private void validateBrowsePaths(final AspectSpec browsePathAspectSpec) {
    assertEquals("testBrowsePaths", browsePathAspectSpec.getName());
    assertEquals(
        new TestBrowsePaths().schema().getFullName(),
        browsePathAspectSpec.getPegasusSchema().getFullName());
    assertEquals(1, browsePathAspectSpec.getSearchableFieldSpecs().size());
    assertEquals(
        SearchableAnnotation.FieldType.BROWSE_PATH,
        browsePathAspectSpec
            .getSearchableFieldSpecs()
            .get(0)
            .getSearchableAnnotation()
            .getFieldType());
  }

  private void validateTestEntityInfo(final AspectSpec testEntityInfo) {
    assertEquals("testEntityInfo", testEntityInfo.getName());
    assertEquals(
        new TestEntityInfo().schema().getFullName(),
        testEntityInfo.getPegasusSchema().getFullName());

    // Assert on Searchable Fields
    assertEquals(testEntityInfo.getSearchableFieldSpecs().size(), 11);
    assertEquals(
        "customProperties",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("customProperties").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.KEYWORD,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("customProperties").toString())
            .getSearchableAnnotation()
            .getFieldType());
    assertEquals(
        "textFieldOverride",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("textField").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.TEXT,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("textField").toString())
            .getSearchableAnnotation()
            .getFieldType());
    assertEquals(
        "textArrayField",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("textArrayField", "*").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.TEXT_PARTIAL,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("textArrayField", "*").toString())
            .getSearchableAnnotation()
            .getFieldType());
    assertEquals(
        "wordGramField",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("wordGramField").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.WORD_GRAM,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("wordGramField").toString())
            .getSearchableAnnotation()
            .getFieldType());
    assertEquals(
        "nestedIntegerField",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("nestedRecordField", "nestedIntegerField").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.COUNT,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("nestedRecordField", "nestedIntegerField").toString())
            .getSearchableAnnotation()
            .getFieldType());
    assertEquals(
        "nestedArrayStringField",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("nestedRecordArrayField", "*", "nestedArrayStringField").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.TEXT,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("nestedRecordArrayField", "*", "nestedArrayStringField").toString())
            .getSearchableAnnotation()
            .getFieldType());
    assertEquals(
        "nestedArrayArrayField",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(
                new PathSpec("nestedRecordArrayField", "*", "nestedArrayArrayField", "*")
                    .toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.TEXT,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(
                new PathSpec("nestedRecordArrayField", "*", "nestedArrayArrayField", "*")
                    .toString())
            .getSearchableAnnotation()
            .getFieldType());
    assertEquals(
        "esObjectField",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("esObjectField").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.OBJECT,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("esObjectField").toString())
            .getSearchableAnnotation()
            .getFieldType());
    assertEquals(
        "foreignKey",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("foreignKey").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        true,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("foreignKey").toString())
            .getSearchableAnnotation()
            .isQueryByDefault());
    assertEquals(
        "doubleField",
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("doubleField").toString())
            .getSearchableAnnotation()
            .getFieldName());
    assertEquals(
        SearchableAnnotation.FieldType.DOUBLE,
        testEntityInfo
            .getSearchableFieldSpecMap()
            .get(new PathSpec("doubleField").toString())
            .getSearchableAnnotation()
            .getFieldType());

    // Assert on Relationship Fields
    assertEquals(4, testEntityInfo.getRelationshipFieldSpecs().size());
    assertEquals(
        "foreignKey",
        testEntityInfo
            .getRelationshipFieldSpecMap()
            .get(new PathSpec("foreignKey").toString())
            .getRelationshipName());
    assertEquals(
        "foreignKeyArray",
        testEntityInfo
            .getRelationshipFieldSpecMap()
            .get(new PathSpec("foreignKeyArray", "*").toString())
            .getRelationshipName());
    assertEquals(
        "nestedForeignKey",
        testEntityInfo
            .getRelationshipFieldSpecMap()
            .get(new PathSpec("nestedRecordField", "nestedForeignKey").toString())
            .getRelationshipName());
    assertEquals(
        "nestedArrayForeignKey",
        testEntityInfo
            .getRelationshipFieldSpecMap()
            .get(new PathSpec("nestedRecordArrayField", "*", "nestedArrayForeignKey").toString())
            .getRelationshipName());
  }

  private void validateSearchFeatures(final AspectSpec searchFeaturesAspectSpec) {
    assertEquals("searchFeatures", searchFeaturesAspectSpec.getName());
    assertEquals(
        new SearchFeatures().schema().getFullName(),
        searchFeaturesAspectSpec.getPegasusSchema().getFullName());
    assertEquals(2, searchFeaturesAspectSpec.getSearchScoreFieldSpecs().size());
    assertEquals(
        "feature1",
        searchFeaturesAspectSpec
            .getSearchScoreFieldSpecMap()
            .get(new PathSpec("feature1").toString())
            .getSearchScoreAnnotation()
            .getFieldName());
    assertEquals(
        "feature2",
        searchFeaturesAspectSpec
            .getSearchScoreFieldSpecMap()
            .get(new PathSpec("feature2").toString())
            .getSearchScoreAnnotation()
            .getFieldName());
  }
}
