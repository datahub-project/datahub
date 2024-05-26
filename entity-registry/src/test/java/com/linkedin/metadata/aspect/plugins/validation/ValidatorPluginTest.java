package com.linkedin.metadata.aspect.plugins.validation;

import static org.testng.Assert.assertEquals;

import com.datahub.test.TestEntityProfile;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
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
        configEntityRegistry.getAllAspectPayloadValidators().stream()
            .filter(validator -> validator.shouldApply(ChangeType.UPSERT, "chart", "status"))
            .collect(Collectors.toList());

    assertEquals(
        validators,
        List.of(
            new TestValidator()
                .setConfig(
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
            new TestValidator()
                .setConfig(
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

  @Getter
  @Setter
  @Accessors(chain = true)
  public static class TestValidator extends AspectPayloadValidator {

    public AspectPluginConfig config;

    @Override
    protected Stream<AspectValidationException> validateProposedAspects(
        @Nonnull Collection<? extends BatchItem> mcpItems,
        @Nonnull RetrieverContext retrieverContext) {
      return mcpItems.stream().map(i -> AspectValidationException.forItem(i, "test error"));
    }

    @Override
    protected Stream<AspectValidationException> validatePreCommitAspects(
        @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
      return Stream.empty();
    }
  }
}
