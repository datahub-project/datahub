package com.datahub.plugins.metadata.aspect;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.datahub.test.TestEntityProfile;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCLSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Configuration
public class SpringPluginFactoryTest {

  public static String REGISTRY_FILE_1 = "test-entity-registry-plugins-1.yml";
  public static String REGISTRY_SPRING_FILE_1 = "test-entity-registry-spring-plugins-1.yml";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testMergedEntityRegistryWithSpringPluginFactory() throws EntityRegistryException {
    ConfigEntityRegistry configEntityRegistry1 =
        new ConfigEntityRegistry(
            TestEntityProfile.class.getClassLoader().getResourceAsStream(REGISTRY_FILE_1));
    ConfigEntityRegistry configEntityRegistry2 =
        new ConfigEntityRegistry(
            TestEntityProfile.class.getClassLoader().getResourceAsStream(REGISTRY_SPRING_FILE_1),
            (config, classLoaders) ->
                new SpringPluginFactory(
                    null, config, List.of(SpringPluginFactoryTest.class.getClassLoader())));

    MergedEntityRegistry mergedEntityRegistry = new MergedEntityRegistry(configEntityRegistry1);
    mergedEntityRegistry.apply(configEntityRegistry2);

    Map<String, EntitySpec> entitySpecs = mergedEntityRegistry.getEntitySpecs();
    Map<String, EventSpec> eventSpecs = mergedEntityRegistry.getEventSpecs();
    assertEquals(entitySpecs.values().size(), 2);
    assertEquals(eventSpecs.values().size(), 1);

    EntitySpec entitySpec = mergedEntityRegistry.getEntitySpec("dataset");
    assertEquals(entitySpec.getName(), "dataset");
    assertEquals(entitySpec.getKeyAspectSpec().getName(), "datasetKey");
    assertEquals(entitySpec.getAspectSpecs().size(), 4);
    assertNotNull(entitySpec.getAspectSpec("datasetKey"));
    assertNotNull(entitySpec.getAspectSpec("datasetProperties"));
    assertNotNull(entitySpec.getAspectSpec("schemaMetadata"));
    assertNotNull(entitySpec.getAspectSpec("status"));

    entitySpec = mergedEntityRegistry.getEntitySpec("chart");
    assertEquals(entitySpec.getName(), "chart");
    assertEquals(entitySpec.getKeyAspectSpec().getName(), "chartKey");
    assertEquals(entitySpec.getAspectSpecs().size(), 3);
    assertNotNull(entitySpec.getAspectSpec("chartKey"));
    assertNotNull(entitySpec.getAspectSpec("chartInfo"));
    assertNotNull(entitySpec.getAspectSpec("status"));

    EventSpec eventSpec = mergedEntityRegistry.getEventSpec("testEvent");
    assertEquals(eventSpec.getName(), "testEvent");
    assertNotNull(eventSpec.getPegasusSchema());

    assertEquals(
        mergedEntityRegistry.getAllAspectPayloadValidators().stream()
            .filter(validator -> validator.shouldApply(ChangeType.UPSERT, "chart", "status"))
            .count(),
        2);
    assertEquals(
        mergedEntityRegistry.getAllAspectPayloadValidators().stream()
            .filter(validator -> validator.shouldApply(ChangeType.DELETE, "chart", "status"))
            .count(),
        1);

    assertEquals(
        mergedEntityRegistry.getAllMCPSideEffects().stream()
            .filter(validator -> validator.shouldApply(ChangeType.UPSERT, "dataset", "datasetKey"))
            .count(),
        2);
    assertEquals(
        mergedEntityRegistry.getAllMCPSideEffects().stream()
            .filter(validator -> validator.shouldApply(ChangeType.DELETE, "dataset", "datasetKey"))
            .count(),
        1);

    assertEquals(
        mergedEntityRegistry.getAllMutationHooks().stream()
            .filter(validator -> validator.shouldApply(ChangeType.UPSERT, "*", "schemaMetadata"))
            .count(),
        2);
    assertEquals(
        mergedEntityRegistry.getAllMutationHooks().stream()
            .filter(validator -> validator.shouldApply(ChangeType.DELETE, "*", "schemaMetadata"))
            .count(),
        1);
  }

  /*
   * Various test plugins to be injected with Spring
   */
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

  @Getter
  @Setter
  @Accessors(chain = true)
  public static class TestMutator extends MutationHook {
    public AspectPluginConfig config;
  }

  @Getter
  @Setter
  @Accessors(chain = true)
  public static class TestMCPSideEffect extends MCPSideEffect {

    public AspectPluginConfig config;

    @Override
    protected Stream<ChangeMCP> applyMCPSideEffect(
        Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
      return changeMCPS.stream();
    }

    @Override
    protected Stream<MCPItem> postMCPSideEffect(
        Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {
      return Stream.of();
    }
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
