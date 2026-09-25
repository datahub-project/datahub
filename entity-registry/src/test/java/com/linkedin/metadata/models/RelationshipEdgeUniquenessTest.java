package com.linkedin.metadata.models;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.StringDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class RelationshipEdgeUniquenessTest {

  @BeforeTest
  public void disableAssert() {
    com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(
            com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor.class
                .getName(),
            false);
  }

  @Test
  public void testDefaultRegistryHasUniqueRelationshipEdges() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            RelationshipEdgeUniquenessTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));

    List<RelationshipEdgeUniquenessValidator.Conflict> conflicts =
        RelationshipEdgeUniquenessValidator.findConflicts(configEntityRegistry);

    assertEquals(
        conflicts,
        Collections.emptyList(),
        RelationshipEdgeUniquenessValidator.formatConflicts("entity-registry.yml", conflicts));
  }

  @Test
  public void testSyntheticConflictDetected() {
    AspectSpec aspectA = aspectWithRelationship("aspectA", "OwnedBy", ImmutableList.of("corpuser"));
    AspectSpec aspectB = aspectWithRelationship("aspectB", "OwnedBy", ImmutableList.of("corpuser"));
    EntitySpec entitySpec = new TestEntitySpec("dataset", ImmutableList.of(aspectA, aspectB));

    List<RelationshipEdgeUniquenessValidator.Conflict> conflicts =
        RelationshipEdgeUniquenessValidator.findConflicts(entitySpec);
    assertEquals(conflicts.size(), 1);
    assertEquals(conflicts.get(0).getRelationshipName(), "OwnedBy");
    assertEquals(conflicts.get(0).getDestinationEntity(), "corpuser");
    assertEquals(conflicts.get(0).getFirstAspect(), "aspectA");
    assertEquals(conflicts.get(0).getSecondAspect(), "aspectB");

    ModelValidationException thrown =
        expectThrows(
            ModelValidationException.class,
            () -> RelationshipEdgeUniquenessValidator.validate(entitySpec));
    assertTrue(thrown.getMessage().contains("OwnedBy"));
  }

  @Test
  public void testSameAspectMultipleFieldsAllowed() {
    AspectSpec aspect =
        aspectWithRelationships(
            "ownership",
            ImmutableList.of(
                relationshipField("owners", "OwnedBy", ImmutableList.of("corpuser")),
                relationshipField("ownerEdges", "OwnedBy", ImmutableList.of("corpuser"))));
    EntitySpec entitySpec = new TestEntitySpec("dataset", ImmutableList.of(aspect));
    assertEquals(
        RelationshipEdgeUniquenessValidator.findConflicts(entitySpec), Collections.emptyList());
  }

  @Test
  public void testWildcardConflictsWithConcreteDestination() {
    AspectSpec concrete =
        aspectWithRelationship("aspectA", "RelatedTo", ImmutableList.of("dataset"));
    AspectSpec wildcard = aspectWithRelationship("aspectB", "RelatedTo", Collections.emptyList());
    EntitySpec entitySpec = new TestEntitySpec("chart", ImmutableList.of(concrete, wildcard));

    List<RelationshipEdgeUniquenessValidator.Conflict> conflicts =
        RelationshipEdgeUniquenessValidator.findConflicts(entitySpec);
    assertEquals(conflicts.size(), 1);
    assertEquals(conflicts.get(0).getRelationshipName(), "RelatedTo");
  }

  @Test
  public void testBuildConfigEntitySpecRejectsConflictingEdges() {
    AspectSpec aspectA = aspectWithRelationship("aspectA", "OwnedBy", ImmutableList.of("corpuser"));
    AspectSpec aspectB = aspectWithRelationship("aspectB", "OwnedBy", ImmutableList.of("corpuser"));
    EntitySpecBuilder builder = new EntitySpecBuilder();

    ModelValidationException thrown =
        expectThrows(
            ModelValidationException.class,
            () ->
                builder.buildConfigEntitySpec(
                    "dataset", "datasetKey", ImmutableList.of(aspectA, aspectB), "primary"));
    assertTrue(thrown.getMessage().contains("OwnedBy"));
  }

  @Test
  public void testBuildPartialEntitySpecRejectsConflictingEdges() {
    AspectSpec aspectA = aspectWithRelationship("aspectA", "OwnedBy", ImmutableList.of("corpuser"));
    AspectSpec aspectB = aspectWithRelationship("aspectB", "OwnedBy", ImmutableList.of("corpuser"));
    EntitySpecBuilder builder = new EntitySpecBuilder();

    ModelValidationException thrown =
        expectThrows(
            ModelValidationException.class,
            () ->
                builder.buildPartialEntitySpec(
                    "dataset", "datasetKey", ImmutableList.of(aspectA, aspectB)));
    assertTrue(thrown.getMessage().contains("OwnedBy"));
  }

  @Test
  public void testBuildConfigAndPartialEntitySpecAllowUniqueEdges() {
    AspectSpec ownership =
        aspectWithRelationship("ownership", "OwnedBy", ImmutableList.of("corpuser"));
    EntitySpecBuilder builder = new EntitySpecBuilder();

    EntitySpec configSpec =
        builder.buildConfigEntitySpec(
            "dataset", "datasetKey", ImmutableList.of(ownership), "primary");
    assertEquals(configSpec.getName(), "dataset");

    EntitySpec partialSpec =
        builder.buildPartialEntitySpec("dataset", "datasetKey", ImmutableList.of(ownership));
    assertEquals(partialSpec.getName(), "dataset");
  }

  private static AspectSpec aspectWithRelationship(
      String aspectName, String relationshipName, List<String> destTypes) {
    return aspectWithRelationships(
        aspectName, ImmutableList.of(relationshipField("field", relationshipName, destTypes)));
  }

  private static AspectSpec aspectWithRelationships(
      String aspectName, List<RelationshipFieldSpec> fields) {
    AspectSpec aspect =
        new AspectSpec(
            new com.linkedin.metadata.models.annotation.AspectAnnotation(
                aspectName, false, false, null, 1L),
            Collections.emptyList(),
            Collections.emptyList(),
            fields,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null);
    return aspect;
  }

  private static RelationshipFieldSpec relationshipField(
      String path, String relationshipName, List<String> destTypes) {
    return new RelationshipFieldSpec(
        new PathSpec(path),
        new RelationshipAnnotation(
            relationshipName, destTypes, true, false, null, null, null, null, null, null),
        new StringDataSchema());
  }

  private static class TestEntitySpec implements EntitySpec {
    private final String name;
    private final List<AspectSpec> aspectSpecs;

    TestEntitySpec(String name, List<AspectSpec> aspectSpecs) {
      this.name = name;
      this.aspectSpecs = aspectSpecs;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public EntityAnnotation getEntityAnnotation() {
      return new EntityAnnotation(name, "keyAspect");
    }

    @Override
    public String getKeyAspectName() {
      return "keyAspect";
    }

    @Override
    public AspectSpec getKeyAspectSpec() {
      return aspectSpecs.isEmpty() ? null : aspectSpecs.get(0);
    }

    @Override
    public List<AspectSpec> getAspectSpecs() {
      return aspectSpecs;
    }

    @Override
    public Map<String, AspectSpec> getAspectSpecMap() {
      return aspectSpecs.stream().collect(Collectors.toMap(AspectSpec::getName, spec -> spec));
    }

    @Override
    public Boolean hasAspect(String aspectName) {
      return aspectSpecs.stream().anyMatch(spec -> spec.getName().equals(aspectName));
    }

    @Override
    public AspectSpec getAspectSpec(String aspectName) {
      return aspectSpecs.stream()
          .filter(spec -> spec.getName().equals(aspectName))
          .findFirst()
          .orElse(null);
    }

    @Override
    public RecordDataSchema getSnapshotSchema() {
      return null;
    }

    @Override
    public TyperefDataSchema getAspectTyperefSchema() {
      return null;
    }

    @Override
    public String getSearchGroup() {
      return "testGroup";
    }
  }
}
