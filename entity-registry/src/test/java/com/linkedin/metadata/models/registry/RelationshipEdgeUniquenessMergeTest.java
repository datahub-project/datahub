package com.linkedin.metadata.models.registry;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.StringDataSchema;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.ConfigEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

public class RelationshipEdgeUniquenessMergeTest {

  @Test
  public void testMergeThrowsWhenPatchAddsConflictingRelationshipAspect() throws Exception {
    AspectSpec baseAspect =
        aspectWithRelationship("aspectA", "OwnedBy", ImmutableList.of("corpuser"));
    AspectSpec patchAspect =
        aspectWithRelationship("aspectB", "OwnedBy", ImmutableList.of("corpuser"));

    EntitySpec baseEntity =
        new ConfigEntitySpec(
            "dataset", "datasetKey", ImmutableList.of(keyAspect(), baseAspect), "primary");
    // Patch only adds the conflicting aspect — omit keyAspect so schema compatibility is not
    // checked against a null-schema duplicate key aspect.
    EntitySpec patchEntity =
        new ConfigEntitySpec("dataset", "datasetKey", ImmutableList.of(patchAspect), "primary");

    MergedEntityRegistry merged = new MergedEntityRegistry(registryOf(baseEntity));
    RelationshipEdgeUniquenessException thrown =
        expectThrows(
            RelationshipEdgeUniquenessException.class, () -> merged.apply(registryOf(patchEntity)));
    assertTrue(thrown.getMessage().contains("OwnedBy"));
    // Registry must remain unchanged (only key + base aspect present)
    assertEquals(merged.getEntitySpec("dataset").getAspectSpecMap().keySet().size(), 2);
    assertTrue(merged.getEntitySpec("dataset").hasAspect("aspectA"));
    assertTrue(!merged.getEntitySpec("dataset").hasAspect("aspectB"));
  }

  @Test
  public void testMergeSucceedsWhenPatchAddsNonConflictingAspect() throws EntityRegistryException {
    AspectSpec baseAspect =
        aspectWithRelationship("aspectA", "OwnedBy", ImmutableList.of("corpuser"));
    AspectSpec patchAspect =
        aspectWithRelationship("aspectB", "Consumes", ImmutableList.of("dataset"));

    EntitySpec baseEntity =
        new ConfigEntitySpec(
            "dataset", "datasetKey", ImmutableList.of(keyAspect(), baseAspect), "primary");
    EntitySpec patchEntity =
        new ConfigEntitySpec("dataset", "datasetKey", ImmutableList.of(patchAspect), "primary");

    MergedEntityRegistry merged = new MergedEntityRegistry(registryOf(baseEntity));
    merged.apply(registryOf(patchEntity));
    assertTrue(merged.getEntitySpec("dataset").hasAspect("aspectA"));
    assertTrue(merged.getEntitySpec("dataset").hasAspect("aspectB"));
  }

  @Test
  public void testPluginLoaderAlwaysFailsOnUniquenessEvenWhenIgnoringFailures()
      throws InterruptedException, EntityRegistryException {
    MergedEntityRegistry mergedEntityRegistry = mock(MergedEntityRegistry.class);
    when(mergedEntityRegistry.apply(any(EntityRegistry.class)))
        .thenThrow(
            new RelationshipEdgeUniquenessException(
                "corpgroup --OwnedBy--> corpuser claimed by aspects 'a' and 'b'"));

    PluginEntityRegistryLoader pluginEntityRegistryLoader =
        new PluginEntityRegistryLoader(TestConstants.BASE_DIRECTORY, 60, true, null)
            .withBaseRegistry(mergedEntityRegistry);

    try {
      pluginEntityRegistryLoader.start(true);
      fail("Expected RuntimeException for relationship edge uniqueness violation");
    } catch (RuntimeException e) {
      assertTrue(
          e.getCause() instanceof RelationshipEdgeUniquenessException,
          "Expected uniqueness cause, got: " + e.getCause());
    }
  }

  private static AspectSpec keyAspect() {
    return new AspectSpec(
        new AspectAnnotation("datasetKey", false, false, null, 1L),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        null);
  }

  private static AspectSpec aspectWithRelationship(
      String aspectName, String relationshipName, List<String> destTypes) {
    RelationshipFieldSpec field =
        new RelationshipFieldSpec(
            new PathSpec("field"),
            new RelationshipAnnotation(
                relationshipName, destTypes, true, false, null, null, null, null, null, null),
            new StringDataSchema());
    return new AspectSpec(
        new AspectAnnotation(aspectName, false, false, null, 1L),
        Collections.emptyList(),
        Collections.emptyList(),
        ImmutableList.of(field),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        null);
  }

  private static EntityRegistry registryOf(EntitySpec entitySpec) {
    Map<String, EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put(entitySpec.getName().toLowerCase(), entitySpec);
    return new EntityRegistry() {
      @Nonnull
      @Override
      public EntitySpec getEntitySpec(@Nonnull String entityName) {
        return entitySpecs.get(entityName.toLowerCase());
      }

      @Nonnull
      @Override
      public EventSpec getEventSpec(@Nonnull String eventName) {
        throw new IllegalArgumentException(eventName);
      }

      @Nonnull
      @Override
      public Map<String, EntitySpec> getEntitySpecs() {
        return entitySpecs;
      }

      @Nonnull
      @Override
      public Map<String, AspectSpec> getAspectSpecs() {
        return new HashMap<>();
      }

      @Nonnull
      @Override
      public Map<String, EventSpec> getEventSpecs() {
        return Collections.emptyMap();
      }

      @Nonnull
      @Override
      public AspectTemplateEngine getAspectTemplateEngine() {
        return new AspectTemplateEngine();
      }
    };
  }
}
