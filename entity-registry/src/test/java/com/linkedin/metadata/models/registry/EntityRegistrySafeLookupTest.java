package com.linkedin.metadata.models.registry;

import static org.testng.Assert.*;

import com.datahub.test.TestEntityProfile;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.EntitySpec;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class EntityRegistrySafeLookupTest {

  private ConfigEntityRegistry registry;

  @BeforeClass
  public void setUp() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    registry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yml"));
  }

  @Test
  public void testKnownEntityAndAspectResolve() {
    assertTrue(registry.findEntitySpec("dataset").isPresent());
    assertEquals(registry.findAspectSpec("dataset", "status").get().getName(), "status");
  }

  @Test
  public void testUnknownAspectOnKnownEntityIsEmpty() {
    assertTrue(registry.findAspectSpec("dataset", "aspectFromNewerBuild").isEmpty());
  }

  @Test
  public void testUnknownEntityIsEmptyForThrowingRegistry() {
    // ConfigEntityRegistry throws IllegalArgumentException for unknown entity names
    assertThrows(IllegalArgumentException.class, () -> registry.getEntitySpec("entityFromNewer"));

    assertTrue(registry.findEntitySpec("entityFromNewer").isEmpty());
    assertTrue(registry.findAspectSpec("entityFromNewer", "status").isEmpty());
  }

  @Test
  public void testUnknownEntityIsEmptyForNullReturningRegistry() {
    assertTrue(EmptyEntityRegistry.EMPTY.findEntitySpec("dataset").isEmpty());
    assertTrue(EmptyEntityRegistry.EMPTY.findAspectSpec("dataset", "status").isEmpty());
  }

  @Test
  public void testOtherRegistryFailuresPropagate() {
    EntityRegistry brokenRegistry =
        new EmptyEntityRegistry() {
          @Override
          public EntitySpec getEntitySpec(String entityName) {
            throw new IllegalStateException("registry not initialized");
          }
        };

    assertThrows(IllegalStateException.class, () -> brokenRegistry.findEntitySpec("dataset"));
    assertThrows(
        IllegalStateException.class, () -> brokenRegistry.findAspectSpec("dataset", "status"));
  }
}
