package com.linkedin.metadata.aspect.plugins.validation;

import static org.testng.Assert.assertEquals;

import com.datahub.test.TestEntityProfile;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ValidatorPluginTest {
  public static String REGISTRY_FILE = "test-entity-registry-plugins-1.yml";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testCustomValidator() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class.getClassLoader().getResourceAsStream(REGISTRY_FILE));

    List<AspectPayloadValidator> validators =
        configEntityRegistry.getAspectPayloadValidators(ChangeType.UPSERT, "*", "status");
    assertEquals(
        validators,
        List.of(
            new TestValidator(
                AspectPluginConfig.builder()
                    .className(
                        "com.linkedin.metadata.aspect.plugins.validation.ValidatorPluginTest$TestValidator")
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
                        "com.linkedin.metadata.aspect.plugins.validation.ValidatorPluginTest$TestValidator")
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
    protected void validateProposedAspect(
        @Nonnull ChangeType changeType,
        @Nonnull Urn entityUrn,
        @Nonnull AspectSpec aspectSpec,
        @Nonnull RecordTemplate aspectPayload,
        AspectRetriever aspectRetriever)
        throws AspectValidationException {
      if (entityUrn.toString().contains("dataset")) {
        throw new AspectValidationException("test error");
      }
    }

    @Override
    protected void validatePreCommitAspect(
        @Nonnull ChangeType changeType,
        @Nonnull Urn entityUrn,
        @Nonnull AspectSpec aspectSpec,
        @Nullable RecordTemplate previousAspect,
        @Nonnull RecordTemplate proposedAspect,
        AspectRetriever aspectRetriever)
        throws AspectValidationException {}
  }
}
