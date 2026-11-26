package com.linkedin.metadata.models.annotation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import java.io.InputStream;
import java.util.Optional;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/** The shipped model, not a hand-built annotation, resolves each degree. */
public class SearchableDegreeRegistryTest {

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testSnapshotRegistryResolvesDegrees() {
    assertResolvedDegrees(SnapshotEntityRegistry.getInstance());
  }

  @Test
  public void testConfigRegistryResolvesDegrees() {
    InputStream config =
        SearchableDegreeRegistryTest.class
            .getClassLoader()
            .getResourceAsStream("entity-registry.yml");
    assertNotNull(config, "entity-registry.yml");
    ConfigEntityRegistry registry = new ConfigEntityRegistry(config);
    assertResolvedDegrees(registry);
    assertNotNull(find(registry, "businessAttribute"));
    assertNotNull(find(registry, "api"));
    assertNotNull(find(registry, "glossaryTerm"));
    assertNameAndDescription(registry, "entityType", "entityTypeInfo");
    assertNameAndDescription(registry, "dataType", "dataTypeInfo");
    assertNameAndDescription(registry, "structuredProperty", "propertyDefinition");
    assertDescription(registry, "dataHubView", "dataHubViewInfo", "description");
    assertDescription(registry, "form", "formInfo", "description");
    assertDescription(registry, "lifecycleStageType", "lifecycleStageTypeInfo", "description");
    assertDescription(registry, "ownershipType", "ownershipTypeInfo", "description");
    assertDescription(registry, "query", "queryProperties", "description");
    assertDescription(registry, "role", "roleProperties", "description");
    assertDescription(registry, "mlPrimaryKey", "mlPrimaryKeyProperties", "description");
    assertDescription(
        registry, "mlPrimaryKey", "editableMlPrimaryKeyProperties", "editedDescription");
    assertDescription(registry, "dataHubSecret", "dataHubSecretValue", "description");
    assertDescription(registry, "dataHubAccessToken", "dataHubAccessTokenInfo", "description");
    assertDescription(registry, "post", "postInfo", "description");
    assertName(registry, "corpuser", "slackUserInfo", "slackDisplayName");
    assertName(registry, "dataPlatform", "dataPlatformInfo", "displayName");
  }

  private static void assertResolvedDegrees(EntityRegistry registry) {
    EntitySpec dataset = registry.getEntitySpec("dataset");
    assertDegree(field(dataset, "datasetProperties", "description"), "description", 1.0, 1.0, 0);
    assertDegree(field(dataset, "schemaMetadata", "fieldPaths"), "name_1", 1.0, 1.0, 1);
    assertTrue(field(dataset, "schemaMetadata", "fieldPaths").isQueryByDefault());
    assertFalse(field(dataset, "schemaMetadata", "fieldPaths").resolvedForV3().isQueryByDefault());
    assertDegree(field(dataset, "schemaMetadata", "fieldLabels"), "name_1", 1.0, 1.0, 1);
    assertTrue(field(dataset, "schemaMetadata", "fieldLabels").isQueryByDefault());
    assertFalse(field(dataset, "schemaMetadata", "fieldLabels").resolvedForV3().isQueryByDefault());
    assertDegree(
        field(dataset, "schemaMetadata", "fieldDescriptions"), "description_1", 0.5, 1.0, 1);
    assertEquals(
        field(dataset, "datasetProperties", "description").indexedHasValuesFieldName(),
        Optional.of("hasDescription"));
    assertEquals(
        field(dataset, "schemaMetadata", "fieldDescriptions").getHasValuesFieldName(),
        Optional.of("hasDescription"));
    assertEquals(
        field(dataset, "schemaMetadata", "fieldDescriptions")
            .resolvedForV3()
            .indexedHasValuesFieldName(),
        Optional.of("hasDescription_1"));
    assertEquals(
        field(dataset, "schemaMetadata", "fieldTags").resolvedForV3().indexedHasValuesFieldName(),
        Optional.of("hasTags_1"));
    assertDegree(
        field(dataset, "editableSchemaMetadata", "editedFieldDescriptions"),
        "description_1",
        0.5,
        1.0,
        1);
    assertTrue(
        field(dataset, "editableSchemaMetadata", "editedFieldDescriptions").isQueryByDefault());
    assertFalse(
        field(dataset, "editableSchemaMetadata", "editedFieldDescriptions")
            .resolvedForV3()
            .isQueryByDefault());
    assertDegree(field(dataset, "globalTags", "tags"), "tags", 0.5, 0.5, 0);
    assertDegree(field(dataset, "schemaMetadata", "fieldTags"), "tags_1", 0.25, 0.5, 1);
    assertDegree(
        field(dataset, "editableSchemaMetadata", "editedFieldTags"), "tags_1", 0.25, 0.5, 1);
    assertDegree(field(dataset, "glossaryTerms", "glossaryTerms"), "glossaryTerms", 0.5, 0.5, 0);
    assertDegree(
        field(dataset, "schemaMetadata", "fieldGlossaryTerms"), "glossaryTerms_1", 0.25, 0.5, 1);
    SearchableAnnotation editedTerms =
        field(dataset, "editableSchemaMetadata", "editedFieldGlossaryTerms");
    assertDegree(editedTerms, "glossaryTerms_1", 0.25, 0.5, 1);
    assertEquals(editedTerms.getV3IncludeSystemModifiedAt(), Optional.of(false));
    assertFalse(editedTerms.resolvedForV3().isIncludeSystemModifiedAt());

    // The legacy snapshot union does not include every entity the config registry does.
    EntitySpec businessAttribute = find(registry, "businessAttribute");
    if (businessAttribute != null) {
      assertDegree(
          field(businessAttribute, "businessAttributeInfo", "editedFieldDescriptions"),
          "description",
          1.0,
          1.0,
          0);
      assertTrue(
          field(businessAttribute, "businessAttributeInfo", "editedFieldDescriptions")
              .isQueryByDefault());
      assertDegree(
          field(businessAttribute, "businessAttributeInfo", "editedFieldTags"),
          "tags",
          0.5,
          0.5,
          0);
      assertDegree(
          field(businessAttribute, "businessAttributeInfo", "editedFieldGlossaryTerms"),
          "glossaryTerms",
          0.5,
          0.5,
          0);
    }
    EntitySpec api = find(registry, "api");
    if (api != null && api.getAspectSpec("apiSignature") != null) {
      assertDegree(field(api, "apiSignature", "fieldTags"), "tags_1", 0.25, 0.5, 1);
      assertDegree(
          field(api, "apiSignature", "fieldGlossaryTerms"), "glossaryTerms_1", 0.25, 0.5, 1);
    }
    EntitySpec glossaryTerm = find(registry, "glossaryTerm");
    if (glossaryTerm != null && glossaryTerm.getAspectSpec("schemaMetadata") != null) {
      assertDegree(field(glossaryTerm, "schemaMetadata", "fieldTags"), "tags_1", 0.25, 0.5, 1);
      assertDegree(
          field(glossaryTerm, "schemaMetadata", "fieldGlossaryTerms"),
          "glossaryTerms_1",
          0.25,
          0.5,
          1);
    }
  }

  private static EntitySpec find(EntityRegistry registry, String name) {
    if (registry.getEntitySpecs().containsKey(name)) {
      return registry.getEntitySpec(name);
    }
    if (registry.getEntitySpecs().containsKey(name.toLowerCase())) {
      return registry.getEntitySpec(name);
    }
    return null;
  }

  private static void assertNameAndDescription(
      EntityRegistry registry, String entityName, String aspectName) {
    assertName(registry, entityName, aspectName, "displayName");
    assertDescription(registry, entityName, aspectName, "description");
  }

  private static void assertName(
      EntityRegistry registry, String entityName, String aspectName, String fieldName) {
    SearchableAnnotation displayName =
        field(registry.getEntitySpec(entityName), aspectName, fieldName);
    assertDegree(displayName, "name", 10.0, 10.0, 0);
    assertTrue(displayName.isQueryByDefault());
  }

  private static void assertDescription(
      EntityRegistry registry, String entityName, String aspectName, String fieldName) {
    SearchableAnnotation description =
        field(registry.getEntitySpec(entityName), aspectName, fieldName);
    assertDegree(description, "description", 1.0, 1.0, 0);
    assertTrue(description.isQueryByDefault());
    assertEquals(description.indexedHasValuesFieldName(), Optional.of("hasDescription"));
  }

  private static void assertDegree(
      SearchableAnnotation annotation,
      String indexedName,
      double resolvedScore,
      double v2Score,
      int degree) {
    assertEquals(annotation.getBoostScore(), v2Score, annotation.getFieldName());
    assertEquals(annotation.getDegree(), degree, annotation.getFieldName());
    SearchableAnnotation resolved = annotation.resolvedForV3();
    assertEquals(resolved.indexedFieldName().orElse(null), indexedName, annotation.getFieldName());
    assertEquals(resolved.getBoostScore(), resolvedScore, indexedName);
    assertEquals(resolved.getDegree(), degree, indexedName);
  }

  private static SearchableAnnotation field(EntitySpec entity, String aspect, String fieldName) {
    return entity.getAspectSpec(aspect).getSearchableFieldSpecs().stream()
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .filter(annotation -> fieldName.equals(annotation.getFieldName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError(entity.getName() + " " + aspect + " " + fieldName));
  }
}
