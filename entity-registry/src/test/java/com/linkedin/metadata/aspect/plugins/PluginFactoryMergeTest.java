package com.linkedin.metadata.aspect.plugins;

import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.aspect.plugins.hooks.MCLSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

/**
 * Comprehensive tests for {@link PluginFactory#merge(PluginFactory, PluginFactory,
 * java.util.function.BiFunction)} functionality, focusing on duplicate resolution, plugin ordering,
 * and edge cases.
 *
 * <p>These tests ensure that:
 *
 * <ul>
 *   <li>All plugins from factory A are included first, unless disabled by factory B configurations
 *   <li>All plugins from factory B are included second
 *   <li>Factory B can selectively disable specific plugins from factory A using matching disabled
 *       configurations
 *   <li>Empty factories are handled correctly
 *   <li>Plugin configurations are merged properly
 * </ul>
 */
public class PluginFactoryMergeTest {

  /** Test that merging two empty factories returns an empty factory. */
  @Test
  public void testMergeEmptyFactories() {
    PluginFactory emptyA = PluginFactory.empty();
    PluginFactory emptyB = PluginFactory.empty();

    PluginFactory result = PluginFactory.merge(emptyA, emptyB, null);

    assertTrue(result.isEmpty());
  }

  /** Test that merging an empty factory with a non-empty factory returns the non-empty factory. */
  @Test
  public void testMergeEmptyWithNonEmpty() {
    PluginFactory empty = PluginFactory.empty();
    PluginFactory nonEmpty = createSimplePluginFactory("TestValidator");

    // Empty + NonEmpty = NonEmpty
    PluginFactory result1 = PluginFactory.merge(empty, nonEmpty, null);
    assertEquals(result1, nonEmpty);

    // NonEmpty + Empty = NonEmpty
    PluginFactory result2 = PluginFactory.merge(nonEmpty, empty, null);
    assertEquals(result2, nonEmpty);
  }

  /**
   * Test merging two factories with completely different plugins. Should result in all plugins from
   * A followed by all plugins from B.
   */
  @Test
  public void testMergeNonOverlappingPlugins() {
    PluginFactory factoryA = createSimplePluginFactory("ValidatorA");
    PluginFactory factoryB = createSimplePluginFactory("ValidatorB");

    PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);

    // Should have both validators
    List<AspectPayloadValidator> validators = merged.getAspectPayloadValidators();
    assertEquals(validators.size(), 2);

    // Order should be A first, then B
    assertEquals(validators.get(0).getConfig().getClassName(), "ValidatorA");
    assertEquals(validators.get(1).getConfig().getClassName(), "ValidatorB");
  }

  /**
   * Test selective disabling: when factory B has a disabled configuration that matches an enabled
   * plugin in factory A, the plugin from A should be filtered out. Disabled plugins are
   * subsequently filtered out by the constructor, so the result is empty.
   */
  @Test
  public void testDuplicateResolutionDisabling() {
    // Factory A has enabled validator
    PluginFactory factoryA = createPluginFactoryWithValidator("TestValidator", true);

    // Factory B has same validator but disabled - this disables A's validator
    PluginFactory factoryB = createPluginFactoryWithValidator("TestValidator", false);

    PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);

    // Should have no validators: A's enabled version gets filtered out by B's config,
    // and B's disabled version gets filtered out by the constructor's applyDisable() method
    List<AspectPayloadValidator> validators = merged.getAspectPayloadValidators();
    assertEquals(validators.size(), 0);
  }

  /**
   * Test selective disabling with mixed plugins: factory B can disable specific plugins from A
   * while keeping others and adding new ones.
   */
  @Test
  public void testSelectiveDisablingWithMixedPlugins() {
    // Factory A has two enabled validators
    AspectPluginConfig keeperConfig = createConfig("KeeperValidator", true);
    AspectPluginConfig targetConfig = createConfig("TargetValidator", true);
    PluginFactory factoryA = createPluginFactoryWithConfigs(List.of(keeperConfig, targetConfig));

    // Factory B disables TargetValidator and adds a new one
    AspectPluginConfig disablerConfig =
        createConfig("TargetValidator", false); // matches A's TargetValidator but disabled
    AspectPluginConfig newConfig = createConfig("NewValidator", true);
    PluginFactory factoryB = createPluginFactoryWithConfigs(List.of(disablerConfig, newConfig));

    PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);

    // Should have KeeperValidator (from A) and NewValidator (from B)
    // TargetValidator from A should be filtered out, disablerConfig gets filtered by constructor
    List<AspectPayloadValidator> validators = merged.getAspectPayloadValidators();
    assertEquals(validators.size(), 2);

    List<String> classNames =
        validators.stream()
            .map(v -> v.getConfig().getClassName())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(classNames, List.of("KeeperValidator", "NewValidator"));

    // Both remaining validators should be enabled
    assertTrue(validators.stream().allMatch(v -> v.getConfig().isEnabled()));
  }

  /**
   * Test that selective disabling only occurs when plugin configurations are identical except for
   * enabled flag. Different configurations should not trigger filtering - both plugins should be
   * included.
   */
  @Test
  public void testNoDuplicateResolutionForDifferentConfigs() {
    // Factory A: TestValidator with operation "CREATE"
    AspectPluginConfig configA =
        AspectPluginConfig.builder()
            .className("TestValidator")
            .enabled(true)
            .supportedOperations(List.of("CREATE"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    // Factory B: TestValidator with operation "UPDATE" (different config)
    AspectPluginConfig configB =
        AspectPluginConfig.builder()
            .className("TestValidator")
            .enabled(true)
            .supportedOperations(List.of("UPDATE"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    PluginFactory factoryA = createPluginFactoryWithConfig(configA);
    PluginFactory factoryB = createPluginFactoryWithConfig(configB);

    PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);

    // Should have both validators since they have different configurations
    List<AspectPayloadValidator> validators = merged.getAspectPayloadValidators();
    assertEquals(validators.size(), 2);
    assertEquals(validators.get(0).getConfig().getSupportedOperations(), List.of("CREATE"));
    assertEquals(validators.get(1).getConfig().getSupportedOperations(), List.of("UPDATE"));
  }

  /**
   * Test that when multiple plugins from factory A match disabled configurations in factory B, all
   * the matching plugins from A are correctly filtered out, and B's disabled plugins are also
   * filtered out.
   */
  @Test
  public void testMultipleDuplicateResolution() {
    // Factory A has two enabled validators
    AspectPluginConfig configA1 = createConfig("ValidatorA", true);
    AspectPluginConfig configA2 = createConfig("ValidatorB", true);
    PluginFactory factoryA = createPluginFactoryWithConfigs(List.of(configA1, configA2));

    // Factory B has same validators but disabled
    AspectPluginConfig configB1 = createConfig("ValidatorA", false);
    AspectPluginConfig configB2 = createConfig("ValidatorB", false);
    PluginFactory factoryB = createPluginFactoryWithConfigs(List.of(configB1, configB2));

    PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);

    // Should have no validators: A's enabled versions get filtered out by B's configs,
    // and B's disabled versions get filtered out by the constructor's applyDisable() method
    List<AspectPayloadValidator> validators = merged.getAspectPayloadValidators();
    assertEquals(validators.size(), 0);
  }

  /**
   * Test plugin inclusion order: factory A plugins should come first (unless filtered out), then
   * factory B plugins, demonstrating the correct merge sequence.
   */
  @Test
  public void testPluginOrdering() {
    // Factory A: UniqueA (enabled), SharedValidator (enabled)
    AspectPluginConfig uniqueA = createConfig("UniqueA", true);
    AspectPluginConfig sharedEnabled = createConfig("SharedValidator", true);
    PluginFactory factoryA = createPluginFactoryWithConfigs(List.of(uniqueA, sharedEnabled));

    // Factory B: SharedValidator (disabled), UniqueB (enabled)
    AspectPluginConfig sharedDisabled = createConfig("SharedValidator", false);
    AspectPluginConfig uniqueB = createConfig("UniqueB", true);
    PluginFactory factoryB = createPluginFactoryWithConfigs(List.of(sharedDisabled, uniqueB));

    PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);

    List<AspectPayloadValidator> validators = merged.getAspectPayloadValidators();
    assertEquals(validators.size(), 2);

    // Expected order: UniqueA (from A, not filtered), UniqueB (from B)
    // Note: A's SharedValidator is filtered out because B has a matching disabled config
    // B's SharedValidator is filtered out because it's disabled
    assertEquals(validators.get(0).getConfig().getClassName(), "UniqueA");
    assertTrue(validators.get(0).getConfig().isEnabled());

    assertEquals(validators.get(1).getConfig().getClassName(), "UniqueB");
    assertTrue(validators.get(1).getConfig().isEnabled());
  }

  /**
   * Test that the merge operation works correctly for all plugin types: AspectPayloadValidators,
   * MutationHooks, MCLSideEffects, and MCPSideEffects. Should include all plugins from A first,
   * then all plugins from B.
   */
  @Test
  public void testMergeAllPluginTypes() {
    // Create factories with all plugin types
    PluginFactory factoryA = createFactoryWithAllPluginTypes("A");
    PluginFactory factoryB = createFactoryWithAllPluginTypes("B");

    PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);

    // Should have plugins from both factories for each type
    assertEquals(merged.getAspectPayloadValidators().size(), 2);
    assertEquals(merged.getMutationHooks().size(), 2);
    assertEquals(merged.getMclSideEffects().size(), 2);
    assertEquals(merged.getMcpSideEffects().size(), 2);

    // Verify ordering (A plugins first, then B plugins)
    assertEquals(
        merged.getAspectPayloadValidators().get(0).getConfig().getClassName(), "ValidatorA");
    assertEquals(
        merged.getAspectPayloadValidators().get(1).getConfig().getClassName(), "ValidatorB");
  }

  /**
   * Test that PluginConfiguration objects are properly merged by including configurations from both
   * factories.
   */
  @Test
  public void testConfigurationMerging() {
    PluginFactory factoryA = createSimplePluginFactory("ValidatorA");
    PluginFactory factoryB = createSimplePluginFactory("ValidatorB");

    PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);

    // Configuration should include configs from both factories
    PluginConfiguration mergedConfig = merged.getPluginConfiguration();
    assertEquals(mergedConfig.getAspectPayloadValidators().size(), 2);
  }

  // Helper methods for creating test factories and configurations

  private PluginFactory createSimplePluginFactory(String validatorClassName) {
    return createPluginFactoryWithValidator(validatorClassName, true);
  }

  private PluginFactory createPluginFactoryWithValidator(String className, boolean enabled) {
    AspectPluginConfig config = createConfig(className, enabled);
    return createPluginFactoryWithConfig(config);
  }

  private PluginFactory createPluginFactoryWithConfig(AspectPluginConfig config) {
    return createPluginFactoryWithConfigs(List.of(config));
  }

  private PluginFactory createPluginFactoryWithConfigs(List<AspectPluginConfig> configs) {
    // Create mock validators with the given configs
    List<AspectPayloadValidator> validators =
        configs.stream()
            .map(
                config ->
                    (AspectPayloadValidator) new MockAspectPayloadValidator().setConfig(config))
            .collect(java.util.stream.Collectors.toList());

    // Create PluginConfiguration with the same configs - this is crucial for merge logic
    PluginConfiguration pluginConfiguration =
        new PluginConfiguration(
            configs, // aspectPayloadValidators
            Collections.emptyList(), // mutationHooks
            Collections.emptyList(), // mclSideEffects
            Collections.emptyList() // mcpSideEffects
            );

    return new PluginFactory(
        pluginConfiguration,
        Collections.emptyList(),
        validators,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
  }

  private AspectPluginConfig createConfig(String className, boolean enabled) {
    return AspectPluginConfig.builder()
        .className(className)
        .enabled(enabled)
        .supportedOperations(List.of("CREATE", "UPDATE"))
        .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
        .build();
  }

  private PluginFactory createFactoryWithAllPluginTypes(String suffix) {
    AspectPluginConfig config = createConfig("Validator" + suffix, true);

    // Create PluginConfiguration with configs for all plugin types
    PluginConfiguration pluginConfiguration =
        new PluginConfiguration(
            List.of(config), // aspectPayloadValidators
            List.of(config), // mutationHooks
            List.of(config), // mclSideEffects
            List.of(config) // mcpSideEffects
            );

    return new PluginFactory(
        pluginConfiguration,
        Collections.emptyList(),
        List.of((AspectPayloadValidator) new MockAspectPayloadValidator().setConfig(config)),
        List.of((MutationHook) new MockMutationHook().setConfig(config)),
        List.of((MCLSideEffect) new MockMCLSideEffect().setConfig(config)),
        List.of((MCPSideEffect) new MockMCPSideEffect().setConfig(config)));
  }

  // Mock plugin implementations for testing

  private static class MockAspectPayloadValidator extends AspectPayloadValidator {
    private AspectPluginConfig config;

    @Override
    public AspectPluginConfig getConfig() {
      return config;
    }

    @Override
    public PluginSpec setConfig(AspectPluginConfig config) {
      this.config = config;
      return this;
    }

    @Override
    protected Stream<AspectValidationException> validateProposedAspects(
        @Nonnull Collection<? extends BatchItem> mcpItems,
        @Nonnull RetrieverContext retrieverContext) {
      return Stream.empty();
    }

    @Override
    protected Stream<AspectValidationException> validatePreCommitAspects(
        @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
      return Stream.empty();
    }
  }

  private static class MockMutationHook extends MutationHook {
    private AspectPluginConfig config;

    @Override
    public AspectPluginConfig getConfig() {
      return config;
    }

    @Override
    public PluginSpec setConfig(AspectPluginConfig config) {
      this.config = config;
      return this;
    }
  }

  private static class MockMCLSideEffect extends MCLSideEffect {
    private AspectPluginConfig config;

    @Override
    public AspectPluginConfig getConfig() {
      return config;
    }

    @Override
    public PluginSpec setConfig(AspectPluginConfig config) {
      this.config = config;
      return this;
    }

    @Override
    protected Stream<MCLItem> applyMCLSideEffect(
        @Nonnull Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {
      return Stream.empty();
    }
  }

  private static class MockMCPSideEffect extends MCPSideEffect {
    private AspectPluginConfig config;

    @Override
    public AspectPluginConfig getConfig() {
      return config;
    }

    @Override
    public PluginSpec setConfig(AspectPluginConfig config) {
      this.config = config;
      return this;
    }

    @Override
    protected Stream<ChangeMCP> applyMCPSideEffect(
        @Nonnull Collection<ChangeMCP> mcpItems, @Nonnull RetrieverContext retrieverContext) {
      return Stream.empty();
    }

    @Override
    protected Stream<MCPItem> postMCPSideEffect(
        @Nonnull Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {
      return Stream.empty();
    }
  }
}
