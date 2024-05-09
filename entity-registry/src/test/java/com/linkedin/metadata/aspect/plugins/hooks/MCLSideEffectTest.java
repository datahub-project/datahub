package com.linkedin.metadata.aspect.plugins.hooks;

import static org.testng.Assert.assertEquals;

import com.datahub.test.TestEntityProfile;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.MCLItem;
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
        configEntityRegistry.getAllMCLSideEffects().stream()
            .filter(validator -> validator.shouldApply(ChangeType.UPSERT, "chart", "chartInfo"))
            .collect(Collectors.toList());

    assertEquals(
        mclSideEffects,
        List.of(
            new TestMCLSideEffect()
                .setConfig(
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

  @Getter
  @Setter
  @Accessors(chain = true)
  public static class TestMCLSideEffect extends MCLSideEffect {
    public AspectPluginConfig config;

    @Override
    protected Stream<MCLItem> applyMCLSideEffect(
        @Nonnull Collection<MCLItem> batchItems, @Nonnull RetrieverContext retrieverContext) {
      return null;
    }
  }
}
