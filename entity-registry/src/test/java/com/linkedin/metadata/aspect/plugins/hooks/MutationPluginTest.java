package com.linkedin.metadata.aspect.plugins.hooks;

import static org.testng.Assert.assertEquals;

import com.datahub.test.TestEntityProfile;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MutationPluginTest {
  public static String REGISTRY_FILE = "test-entity-registry-plugins-1.yml";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testCustomMutator() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class.getClassLoader().getResourceAsStream(REGISTRY_FILE));

    List<MutationHook> mutators =
        configEntityRegistry.getAllMutationHooks().stream()
            .filter(validator -> validator.shouldApply(ChangeType.UPSERT, "*", "schemaMetadata"))
            .collect(Collectors.toList());

    assertEquals(
        mutators,
        List.of(
            new TestMutator()
                .setConfig(
                    AspectPluginConfig.builder()
                        .className(
                            "com.linkedin.metadata.aspect.plugins.hooks.MutationPluginTest$TestMutator")
                        .supportedOperations(List.of("UPSERT"))
                        .enabled(true)
                        .supportedEntityAspectNames(
                            List.of(
                                AspectPluginConfig.EntityAspectName.builder()
                                    .entityName("*")
                                    .aspectName("schemaMetadata")
                                    .build()))
                        .build())));
  }

  @Getter
  @Setter
  @Accessors(chain = true)
  public static class TestMutator extends MutationHook {
    public AspectPluginConfig config;
  }
}
