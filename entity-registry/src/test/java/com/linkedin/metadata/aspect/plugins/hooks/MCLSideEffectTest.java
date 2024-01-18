package com.linkedin.metadata.aspect.plugins.hooks;

import static org.testng.Assert.assertEquals;

import com.datahub.test.TestEntityProfile;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLBatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MCLSideEffectTest {
  public static String REGISTRY_FILE = "test-entity-registry-plugins-1.yml";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testCustomMCLSideEffect() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class.getClassLoader().getResourceAsStream(REGISTRY_FILE));

    List<MCLSideEffect> mclSideEffects =
        configEntityRegistry.getMCLSideEffects(ChangeType.UPSERT, "chart", "chartInfo");
    assertEquals(
        mclSideEffects,
        List.of(
            new TestMCLSideEffect(
                AspectPluginConfig.builder()
                    .className(
                        "com.linkedin.metadata.aspect.plugins.hooks.MCLSideEffectTest$TestMCLSideEffect")
                    .supportedOperations(List.of("UPSERT"))
                    .enabled(true)
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("chart")
                                .aspectName("chartInfo")
                                .build()))
                    .build())));
  }

  public static class TestMCLSideEffect extends MCLSideEffect {

    public TestMCLSideEffect(AspectPluginConfig aspectPluginConfig) {
      super(aspectPluginConfig);
    }

    @Override
    protected Stream<MCLBatchItem> applyMCLSideEffect(
        @Nonnull MCLBatchItem input,
        @Nonnull EntityRegistry entityRegistry,
        @Nonnull AspectRetriever aspectRetriever) {
      return Stream.of(input);
    }
  }
}
