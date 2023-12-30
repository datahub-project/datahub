package com.linkedin.metadata.aspect.plugins.validation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.datahub.test.TestEntityProfile;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class EntityRegistryValidatorTest {

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testEntityRegistry() throws FileNotFoundException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry-hooks.yml"));

    Map<String, EntitySpec> entitySpecs = configEntityRegistry.getEntitySpecs();
    Map<String, EventSpec> eventSpecs = configEntityRegistry.getEventSpecs();
    assertEquals(entitySpecs.values().size(), 2);
    assertEquals(eventSpecs.values().size(), 1);

    EntitySpec entitySpec = configEntityRegistry.getEntitySpec("dataset");
    assertEquals(entitySpec.getName(), "dataset");
    assertEquals(entitySpec.getKeyAspectSpec().getName(), "datasetKey");
    assertEquals(entitySpec.getAspectSpecs().size(), 4);
    assertNotNull(entitySpec.getAspectSpec("datasetKey"));
    assertNotNull(entitySpec.getAspectSpec("datasetProperties"));
    assertNotNull(entitySpec.getAspectSpec("schemaMetadata"));
    assertNotNull(entitySpec.getAspectSpec("status"));

    entitySpec = configEntityRegistry.getEntitySpec("chart");
    assertEquals(entitySpec.getName(), "chart");
    assertEquals(entitySpec.getKeyAspectSpec().getName(), "chartKey");
    assertEquals(entitySpec.getAspectSpecs().size(), 3);
    assertNotNull(entitySpec.getAspectSpec("chartKey"));
    assertNotNull(entitySpec.getAspectSpec("chartInfo"));
    assertNotNull(entitySpec.getAspectSpec("status"));

    EventSpec eventSpec = configEntityRegistry.getEventSpec("testEvent");
    assertEquals(eventSpec.getName(), "testEvent");
    assertNotNull(eventSpec.getPegasusSchema());
  }

  @Test
  public void testEntityRegistryIdentifier() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry-hooks.yml"));
    assertEquals(configEntityRegistry.getIdentifier(), "test-registry");
  }

  @Test
  public void testCustomValidatorInitialization() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry-hooks.yml"));

    assertEquals(
        configEntityRegistry.getAspectSpecs().get("status").getAspectPayloadValidators().size(), 2);
    assertEquals(
        configEntityRegistry.getAspectSpecs().get("status").getAspectPayloadValidators(),
        List.of(
            new TestValidator(
                AspectPluginConfig.builder()
                    .className(
                        "com.linkedin.metadata.aspect.plugins.validation.EntityRegistryValidatorTest$TestValidator")
                    .supportedOperations(List.of("UPSERT"))
                    .enabled(true)
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("*")
                                .aspectName("status")
                                .build()))
                    .build()),
            new TestValidator(
                AspectPluginConfig.builder()
                    .className(
                        "com.linkedin.metadata.aspect.plugins.validation.EntityRegistryValidatorTest$TestValidator")
                    .supportedOperations(List.of("UPSERT"))
                    .enabled(true)
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("chart")
                                .aspectName("status")
                                .build()))
                    .build())));
  }

  public static class TestValidator extends AspectPayloadValidator {

    public TestValidator(AspectPluginConfig config) {
      super(config);
    }

    @Override
    protected boolean validateProposedAspect(
        @Nonnull ChangeType changeType,
        @Nonnull Urn entityUrn,
        @Nonnull AspectSpec aspectSpec,
        @Nonnull RecordTemplate aspectPayload,
        AspectRetriever aspectRetriever)
        throws AspectValidationException {
      return entityUrn.toString().contains("dataset") ? false : true;
    }

    @Override
    protected boolean validatePreCommitAspect(
        @Nonnull ChangeType changeType,
        @Nonnull Urn entityUrn,
        @Nonnull AspectSpec aspectSpec,
        @Nullable RecordTemplate previousAspect,
        @Nonnull RecordTemplate proposedAspect,
        AspectRetriever aspectRetriever)
        throws AspectValidationException {
      return true;
    }
  }
}
