package com.linkedin.metadata.models;

import com.datahub.test.BrowsePaths;
import com.datahub.test.Snapshot;
import com.datahub.test.TestEntityInfo;
import com.datahub.test.TestEntityKey;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


/**
 * Tests the capabilities of {@link EntitySpecBuilder}
 */
public class EntitySpecBuilderTest {

  @BeforeMethod
  public void init() {
  }

  @Test
  public void testBuildAspectSpecValidation_aspectMissingAnnotation() throws Exception {
    assertEquals();
  }

  @Test
  public void testBuildAspectSpecValidation_invalidSearchableIndexType() throws Exception {
    new EntitySpecBuilder().buildAspectSpec(new TestEntityInfo().schema());
  }

  @Test
  public void testBuildAspectSpecValidation_missingSearchableFieldName() throws Exception {
    new EntitySpecBuilder().buildAspectSpec(new TestEntityInfo().schema());
  }

  @Test
  public void testBuildAspectSpecValidation_missingIndexType() throws Exception {
    new EntitySpecBuilder().buildAspectSpec(new TestEntityInfo().schema());
  }

  @Test
  public void testBuildAspectSpecValidation_invalidSearchableFieldType() throws Exception {
    new EntitySpecBuilder().buildAspectSpec(new TestEntityInfo().schema());
  }

  @Test
  public void testBuildAspectSpecValidation_duplicateSearchableFields() throws Exception {
    new EntitySpecBuilder().buildAspectSpec(new TestEntityInfo().schema());
  }

  @Test
  public void testBuildAspectSpecValidation_missingRelationshipName() throws Exception {
    new EntitySpecBuilder().buildAspectSpec(new TestEntityInfo().schema());
  }

  @Test
  public void testBuildAspectSpecValidation_missingEntityTypes() throws Exception {
    new EntitySpecBuilder().buildAspectSpec(new TestEntityInfo().schema());
  }

  @Test
  public void testBuildAspectSpecValidation_invalidRelationshipFieldType() throws Exception {
    new EntitySpecBuilder().buildAspectSpec(new TestEntityInfo().schema());
  }

  @Test
  public void testBuildEntitySpecs() {

    // Instantiate the test Snapshot
    final Snapshot snapshot = new Snapshot();
    final List<EntitySpec> validEntitySpecs = new EntitySpecBuilder().buildEntitySpecs(snapshot.schema());

    // Assert single entity.
    assertEquals(1, validEntitySpecs.size());

    // Assert on Entity Spec
    final EntitySpec testEntitySpec = validEntitySpecs.get(0);
    assertEquals("testEntity", testEntitySpec.getName());
    assertEquals(Boolean.TRUE, testEntitySpec.isBrowsable());
    assertEquals(Boolean.TRUE, testEntitySpec.isSearchable());

    // Assert on Aspect Specs
    final Map<String, AspectSpec> aspectSpecMap = testEntitySpec.getAspectSpecMap();
    assertEquals(3, aspectSpecMap.size());
    assertTrue(aspectSpecMap.containsKey("testEntityKey"));
    assertTrue(aspectSpecMap.containsKey("browsePaths"));
    assertTrue(aspectSpecMap.containsKey("testEntityInfo"));

    // Assert on TestEntityKey
    validateTestEntityKey(aspectSpecMap.get("testEntityKey"));

    // Assert on BrowsePaths Aspect
    validateBrowsePaths(aspectSpecMap.get("browsePaths"));

    // Assert on TestEntityInfo Aspect
    validateTestEntityInfo(aspectSpecMap.get("testEntityInfo"));
  }

  private void validateTestEntityKey(final AspectSpec keyAspectSpec) {
    assertEquals(Boolean.TRUE, keyAspectSpec.isKey());
    assertEquals("testEntityKey", keyAspectSpec.getName());
    assertEquals(new TestEntityKey().schema().getFullName(), keyAspectSpec.getPegasusSchema().getFullName());

    // Assert on Searchable Fields
    assertEquals(2, keyAspectSpec.getSearchableFieldSpecs().size());
    assertEquals("keyPart1", keyAspectSpec.getSearchableFieldSpecMap().get(new PathSpec("keyPart1").toString()).getFieldName());
    assertEquals(SearchableAnnotation.IndexType.TEXT, keyAspectSpec.getSearchableFieldSpecMap().get(new PathSpec("keyPart1").toString())
        .getIndexSettings().get(0).getIndexType());
    assertEquals("keyPart3", keyAspectSpec.getSearchableFieldSpecMap().get(new PathSpec("keyPart3").toString()).getFieldName());
    assertEquals(SearchableAnnotation.IndexType.KEYWORD, keyAspectSpec.getSearchableFieldSpecMap().get(new PathSpec("keyPart3").toString())
        .getIndexSettings().get(0).getIndexType());

    // Assert on Relationship Field
    assertEquals(1, keyAspectSpec.getRelationshipFieldSpecs().size());
    assertEquals("keyForeignKey", keyAspectSpec.getRelationshipFieldSpecMap().get(new PathSpec("keyPart2").toString()).getRelationshipName());
  }


  private void validateBrowsePaths(final AspectSpec browsePathAspectSpec) {
    assertEquals(Boolean.FALSE, browsePathAspectSpec.isKey());
    assertEquals("browsePaths", browsePathAspectSpec.getName());
    assertEquals(new BrowsePaths().schema().getFullName(), browsePathAspectSpec.getPegasusSchema().getFullName());
    assertEquals(1, browsePathAspectSpec.getSearchableFieldSpecs().size());
    assertEquals(SearchableAnnotation.IndexType.BROWSE_PATH, browsePathAspectSpec.getSearchableFieldSpecs().get(0)
        .getIndexSettings().get(0).getIndexType());
  }

  private void validateTestEntityInfo(final AspectSpec testEntityInfo) {
    assertEquals(Boolean.FALSE, testEntityInfo.isKey());
    assertEquals("testEntityInfo", testEntityInfo.getName());
    assertEquals(new TestEntityInfo().schema().getFullName(), testEntityInfo.getPegasusSchema().getFullName());

    // Assert on Searchable Fields
    assertEquals(4, testEntityInfo.getSearchableFieldSpecs().size());
    assertEquals("textField", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("textField").toString()).getFieldName());
    assertEquals(SearchableAnnotation.IndexType.TEXT, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("textField").toString())
        .getIndexSettings().get(0).getIndexType());
    assertEquals("textArrayField", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("textArrayField", "*").toString()).getFieldName());
    assertEquals(SearchableAnnotation.IndexType.PARTIAL, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("textArrayField", "*").toString())
        .getIndexSettings().get(0).getIndexType());
    assertEquals("nestedIntegerField", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordField", "nestedIntegerField").toString()).getFieldName());
    assertEquals(SearchableAnnotation.IndexType.PARTIAL, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordField", "nestedIntegerField").toString())
        .getIndexSettings().get(0).getIndexType());
    assertEquals("nestedArrayIntegerField", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordArrayField", "*", "nestedArrayIntegerField").toString()).getFieldName());
    assertEquals(SearchableAnnotation.IndexType.KEYWORD, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordArrayField", "*", "nestedArrayIntegerField").toString())
        .getIndexSettings().get(0).getIndexType());

    // Assert on Relationship Fields
    assertEquals(4, testEntityInfo.getRelationshipFieldSpecs().size());
    assertEquals("foreignKey", testEntityInfo.getRelationshipFieldSpecMap().get(
        new PathSpec("foreignKey").toString()).getRelationshipName());
    assertEquals("foreignKeyArray", testEntityInfo.getRelationshipFieldSpecMap().get(
        new PathSpec("foreignKeyArray", "*").toString()).getRelationshipName());
    assertEquals("nestedForeignKey", testEntityInfo.getRelationshipFieldSpecMap().get(
        new PathSpec("nestedRecordField", "nestedForeignKey").toString()).getRelationshipName());
    assertEquals("nestedArrayForeignKey", testEntityInfo.getRelationshipFieldSpecMap().get(
        new PathSpec("nestedRecordArrayField", "*", "nestedArrayForeignKey").toString()).getRelationshipName());
  }

}
