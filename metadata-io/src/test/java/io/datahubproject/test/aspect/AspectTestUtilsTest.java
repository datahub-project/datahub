package io.datahubproject.test.aspect;

import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.validation.ConditionalWriteValidator;
import com.linkedin.metadata.aspect.validation.CreateIfNotExistsValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Comprehensive tests for {@link AspectTestUtils} functionality.
 *
 * <p>These tests verify that AspectTestUtils delivers on its advertised functionality:
 *
 * <ul>
 *   <li>Creating test plugin factories with essential validators
 *   <li>Enhancing existing registries while preserving behavior
 *   <li>Proper merge functionality (additive by default)
 *   <li>Ability to selectively disable base registry plugins
 *   <li>Error handling and edge cases
 * </ul>
 */
public class AspectTestUtilsTest {

  /** Test that createTestPluginFactory creates a factory with the expected validators. */
  @Test
  public void testCreateTestPluginFactory() {
    PluginFactory factory = AspectTestUtils.createTestPluginFactory();

    assertNotNull(factory);
    assertFalse(factory.isEmpty());

    // Should have exactly 2 validators
    List<AspectPayloadValidator> validators = factory.getAspectPayloadValidators();
    assertEquals(validators.size(), 2);

    // Should have the expected validator types
    List<String> validatorClasses =
        validators.stream()
            .map(v -> v.getClass().getSimpleName())
            .sorted()
            .collect(Collectors.toList());

    assertEquals(
        validatorClasses, List.of("ConditionalWriteValidator", "CreateIfNotExistsValidator"));

    // All validators should be enabled
    assertTrue(validators.stream().allMatch(v -> v.getConfig().isEnabled()));

    // Should have no other plugin types
    assertEquals(factory.getMutationHooks().size(), 0);
    assertEquals(factory.getMclSideEffects().size(), 0);
    assertEquals(factory.getMcpSideEffects().size(), 0);
  }

  /** Test that the plugin configuration matches the actual plugin instances. */
  @Test
  public void testCreateTestPluginFactoryConfigurationConsistency() {
    PluginFactory factory = AspectTestUtils.createTestPluginFactory();

    PluginConfiguration config = factory.getPluginConfiguration();
    List<AspectPluginConfig> configList = config.getAspectPayloadValidators();
    List<AspectPayloadValidator> validators = factory.getAspectPayloadValidators();

    // Configuration should match the actual validators
    assertEquals(configList.size(), validators.size());

    // Each validator should have a corresponding configuration
    for (int i = 0; i < validators.size(); i++) {
      AspectPayloadValidator validator = validators.get(i);
      AspectPluginConfig expectedConfig = configList.get(i);

      assertEquals(validator.getConfig().getClassName(), expectedConfig.getClassName());
      assertEquals(validator.getConfig().isEnabled(), expectedConfig.isEnabled());
      assertEquals(
          validator.getConfig().getSupportedOperations(), expectedConfig.getSupportedOperations());
    }
  }

  /** Test enhancing an empty registry. */
  @Test
  public void testEnhanceEmptyRegistry() {
    EntityRegistry emptyRegistry = createMockRegistry(PluginFactory.empty());

    EntityRegistry enhanced = AspectTestUtils.enhanceRegistryWithTestPlugins(emptyRegistry);

    assertNotNull(enhanced);
    assertNotSame(enhanced, emptyRegistry); // Should be a new wrapper

    // Should delegate all registry methods to original
    assertEquals(enhanced.getEntitySpecs(), emptyRegistry.getEntitySpecs());
    assertEquals(enhanced.getAspectSpecs(), emptyRegistry.getAspectSpecs());

    // But should have enhanced plugin factory
    PluginFactory enhancedFactory = enhanced.getPluginFactory();
    assertNotSame(enhancedFactory, emptyRegistry.getPluginFactory());

    // Should have the test validators
    assertEquals(enhancedFactory.getAspectPayloadValidators().size(), 2);
  }

  /** Test enhancing a registry that already has plugins - should be additive. */
  @Test
  public void testEnhanceRegistryAdditive() {
    // Create a base registry with one custom validator
    PluginFactory baseFactory = createCustomPluginFactory("BaseValidator", true);
    EntityRegistry baseRegistry = createMockRegistry(baseFactory);

    EntityRegistry enhanced = AspectTestUtils.enhanceRegistryWithTestPlugins(baseRegistry);

    PluginFactory enhancedFactory = enhanced.getPluginFactory();
    List<AspectPayloadValidator> validators = enhancedFactory.getAspectPayloadValidators();

    // Should have base validator + 2 test validators = 3 total
    assertEquals(validators.size(), 3);

    List<String> validatorClasses =
        validators.stream().map(v -> v.getClass().getSimpleName()).collect(Collectors.toList());

    // Base validator should come first (from merge order), then test validators
    assertEquals(validatorClasses.get(0), "MockValidator");
    assertTrue(validatorClasses.contains("CreateIfNotExistsValidator"));
    assertTrue(validatorClasses.contains("ConditionalWriteValidator"));
  }

  /** Test creating a custom test factory that can disable base plugins. */
  @Test
  public void testCustomTestFactoryCanDisableBasePlugins() {
    // Create a base registry with a validator we want to disable
    AspectPluginConfig baseConfig =
        AspectPluginConfig.builder()
            .className("TestTargetValidator")
            .enabled(true)
            .supportedOperations(List.of("CREATE", "UPDATE"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    PluginFactory baseFactory = createPluginFactoryWithConfig(baseConfig);
    EntityRegistry baseRegistry = createMockRegistry(baseFactory);

    // Create a custom test factory that disables the base validator
    AspectPluginConfig disablingConfig =
        AspectPluginConfig.builder()
            .className("TestTargetValidator")
            .enabled(false) // Same config but disabled
            .supportedOperations(List.of("CREATE", "UPDATE"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    PluginFactory customTestFactory = createPluginFactoryWithConfig(disablingConfig);

    // Manually merge to test disabling functionality
    PluginFactory merged = PluginFactory.merge(baseFactory, customTestFactory, null);

    // The base validator should be filtered out (disabled), and the test factory's disabled
    // validator
    // should also be filtered out by constructor, resulting in no validators
    assertEquals(merged.getAspectPayloadValidators().size(), 0);
  }

  /** Test that we can create a test factory that adds validators alongside disabling others. */
  @Test
  public void testSelectiveDisablingWithNewValidators() {
    // Base registry with two validators
    AspectPluginConfig keepConfig =
        AspectPluginConfig.builder()
            .className("KeepValidator")
            .enabled(true)
            .supportedOperations(List.of("CREATE"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    AspectPluginConfig targetConfig =
        AspectPluginConfig.builder()
            .className("TargetValidator")
            .enabled(true)
            .supportedOperations(List.of("UPDATE"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    PluginFactory baseFactory = createPluginFactoryWithConfigs(List.of(keepConfig, targetConfig));

    // Test factory that disables TargetValidator but adds CreateIfNotExistsValidator
    AspectPluginConfig disablerConfig =
        AspectPluginConfig.builder()
            .className("TargetValidator")
            .enabled(false) // Disable the target
            .supportedOperations(List.of("UPDATE"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    AspectPluginConfig newConfig = AspectTestUtils.createCreateIfNotExistsValidatorConfig();

    PluginFactory testFactory = createPluginFactoryWithConfigs(List.of(disablerConfig, newConfig));

    PluginFactory merged = PluginFactory.merge(baseFactory, testFactory, null);

    List<AspectPayloadValidator> validators = merged.getAspectPayloadValidators();

    // Should have KeepValidator (from base) + CreateIfNotExistsValidator (from test)
    // TargetValidator should be disabled and filtered out
    assertEquals(validators.size(), 2);

    List<String> classNames =
        validators.stream()
            .map(v -> v.getConfig().getClassName())
            .sorted()
            .collect(Collectors.toList());

    assertTrue(classNames.contains("KeepValidator"));
    assertTrue(classNames.contains(CreateIfNotExistsValidator.class.getName()));
    assertFalse(classNames.contains("TargetValidator"));
  }

  /** Test null input handling. */
  @Test(
      expectedExceptions = NullPointerException.class,
      expectedExceptionsMessageRegExp = "Base registry cannot be null")
  public void testEnhanceNullRegistry() {
    AspectTestUtils.enhanceRegistryWithTestPlugins(null);
  }

  /** Test that the wrapper delegates all EntityRegistry methods correctly. */
  @Test
  public void testRegistryWrapperDelegation() {
    EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
    PluginFactory mockFactory = PluginFactory.empty();

    Mockito.when(mockRegistry.getPluginFactory()).thenReturn(mockFactory);
    Mockito.when(mockRegistry.getEntitySpecs()).thenReturn(Collections.emptyMap());
    Mockito.when(mockRegistry.getAspectSpecs()).thenReturn(Collections.emptyMap());
    Mockito.when(mockRegistry.getEventSpecs()).thenReturn(Collections.emptyMap());

    EntityRegistry enhanced = AspectTestUtils.enhanceRegistryWithTestPlugins(mockRegistry);

    // Verify delegation
    enhanced.getEntitySpecs();
    enhanced.getAspectSpecs();
    enhanced.getEventSpecs();

    Mockito.verify(mockRegistry).getEntitySpecs();
    Mockito.verify(mockRegistry).getAspectSpecs();
    Mockito.verify(mockRegistry).getEventSpecs();

    // Plugin factory should be enhanced, not the original
    assertNotSame(enhanced.getPluginFactory(), mockFactory);
  }

  /** Test the configuration helper methods create valid configurations. */
  @Test
  public void testConfigurationHelperMethods() {
    AspectPluginConfig createConfig = AspectTestUtils.createCreateIfNotExistsValidatorConfig();
    AspectPluginConfig conditionalConfig = AspectTestUtils.createConditionalWriteValidatorConfig();

    // Verify CreateIfNotExistsValidator config
    assertEquals(createConfig.getClassName(), CreateIfNotExistsValidator.class.getName());
    assertTrue(createConfig.isEnabled());
    assertEquals(createConfig.getSupportedOperations(), List.of("CREATE", "CREATE_ENTITY"));
    assertEquals(
        createConfig.getSupportedEntityAspectNames(),
        List.of(AspectPluginConfig.EntityAspectName.ALL));

    // Verify ConditionalWriteValidator config
    assertEquals(conditionalConfig.getClassName(), ConditionalWriteValidator.class.getName());
    assertTrue(conditionalConfig.isEnabled());
    assertEquals(
        conditionalConfig.getSupportedOperations(),
        List.of("CREATE", "CREATE_ENTITY", "DELETE", "UPSERT", "UPDATE", "PATCH"));
    assertEquals(
        conditionalConfig.getSupportedEntityAspectNames(),
        List.of(AspectPluginConfig.EntityAspectName.ALL));
  }

  // Helper methods for creating test objects

  private EntityRegistry createMockRegistry(PluginFactory pluginFactory) {
    EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
    Mockito.when(mockRegistry.getPluginFactory()).thenReturn(pluginFactory);
    Mockito.when(mockRegistry.getEntitySpecs()).thenReturn(Collections.emptyMap());
    Mockito.when(mockRegistry.getAspectSpecs()).thenReturn(Collections.emptyMap());
    Mockito.when(mockRegistry.getEventSpecs()).thenReturn(Collections.emptyMap());
    return mockRegistry;
  }

  private PluginFactory createCustomPluginFactory(String validatorName, boolean enabled) {
    AspectPluginConfig config =
        AspectPluginConfig.builder()
            .className(validatorName)
            .enabled(enabled)
            .supportedOperations(List.of("CREATE", "UPDATE"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    return createPluginFactoryWithConfig(config);
  }

  private PluginFactory createPluginFactoryWithConfig(AspectPluginConfig config) {
    return createPluginFactoryWithConfigs(List.of(config));
  }

  private PluginFactory createPluginFactoryWithConfigs(List<AspectPluginConfig> configs) {
    List<AspectPayloadValidator> validators =
        configs.stream()
            .map(config -> (AspectPayloadValidator) new MockValidator().setConfig(config))
            .collect(Collectors.toList());

    PluginConfiguration pluginConfiguration =
        new PluginConfiguration(
            configs, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    return new PluginFactory(
        pluginConfiguration,
        Collections.emptyList(),
        validators,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
  }

  /** Mock validator for testing purposes. */
  private static class MockValidator extends AspectPayloadValidator {
    private AspectPluginConfig config;

    @Override
    public AspectPluginConfig getConfig() {
      return config;
    }

    @Override
    public AspectPayloadValidator setConfig(AspectPluginConfig config) {
      this.config = config;
      return this;
    }

    @Override
    protected java.util.stream.Stream<
            com.linkedin.metadata.aspect.plugins.validation.AspectValidationException>
        validateProposedAspects(
            @Nonnull
                java.util.Collection<? extends com.linkedin.metadata.aspect.batch.BatchItem>
                    mcpItems,
            @Nonnull com.linkedin.metadata.aspect.RetrieverContext retrieverContext) {
      return java.util.stream.Stream.empty();
    }

    @Override
    protected java.util.stream.Stream<
            com.linkedin.metadata.aspect.plugins.validation.AspectValidationException>
        validatePreCommitAspects(
            @Nonnull java.util.Collection<com.linkedin.metadata.aspect.batch.ChangeMCP> changeMCPs,
            @Nonnull com.linkedin.metadata.aspect.RetrieverContext retrieverContext) {
      return java.util.stream.Stream.empty();
    }
  }
}
