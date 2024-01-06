package com.linkedin.metadata.aspect.plugins.hooks;

import static org.testng.Assert.assertEquals;

import com.datahub.test.TestEntityProfile;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.UpsertItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MCPSideEffectTest {
  public static String REGISTRY_FILE = "test-entity-registry-plugins-1.yml";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testCustomMCPSideEffect() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class.getClassLoader().getResourceAsStream(REGISTRY_FILE));

    List<MCPSideEffect> mcpSideEffects =
        configEntityRegistry.getMCPSideEffects(ChangeType.UPSERT, "dataset", "datasetKey");
    assertEquals(
        mcpSideEffects,
        List.of(
            new MCPSideEffectTest.TestMCPSideEffect(
                AspectPluginConfig.builder()
                    .className(
                        "com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffectTest$TestMCPSideEffect")
                    .supportedOperations(List.of("UPSERT"))
                    .enabled(true)
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("dataset")
                                .aspectName("datasetKey")
                                .build()))
                    .build())));
  }

  public static class TestMCPSideEffect extends MCPSideEffect {

    public TestMCPSideEffect(AspectPluginConfig aspectPluginConfig) {
      super(aspectPluginConfig);
    }

    @Override
    protected Stream<UpsertItem> applyMCPSideEffect(
        UpsertItem input, EntityRegistry entityRegistry, @Nonnull AspectRetriever aspectRetriever) {
      return Stream.of(input);
    }
  }
}
