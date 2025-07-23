package io.datahubproject.test.aspect;

import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.validation.ConditionalWriteValidator;
import com.linkedin.metadata.aspect.validation.CreateIfNotExistsValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.List;

/**
 * Utilities for configuring aspect validation plugins in DataHub tests.
 *
 * <h2>Background</h2>
 *
 * DataHub uses an aspect-based metadata model where entities are composed of multiple aspects
 * (e.g., ownership, schema, properties). The platform includes a plugin system for validating and
 * transforming these aspects during write operations.
 *
 * <h2>Plugin System Architecture</h2>
 *
 * <ul>
 *   <li><strong>Production:</strong> Uses Spring-based plugin discovery from YAML configuration
 *   <li><strong>Tests:</strong> Use programmatic plugin configuration to avoid Spring context
 *       overhead
 * </ul>
 *
 * <h2>Testing Challenge</h2>
 *
 * Production uses {@code SpringStandardPluginConfiguration} which automatically discovers and
 * configures plugins from YAML files. However, tests use {@code ConfigEntityRegistry} which
 * requires explicit plugin configuration. This class bridges that gap by providing programmatically
 * configured essential validators for testing.
 *
 * <h2>Essential Validators</h2>
 *
 * This utility focuses on two critical validators that many tests depend on:
 *
 * <ul>
 *   <li><strong>CreateIfNotExistsValidator:</strong> Ensures CREATE operations fail if aspect
 *       already exists
 *   <li><strong>ConditionalWriteValidator:</strong> Validates conditional write headers (version
 *       checks, etc.)
 * </ul>
 *
 * <h2>Plugin Factory Merging</h2>
 *
 * <p>This utility relies on {@link com.linkedin.metadata.aspect.plugins.PluginFactory#merge} to
 * combine the base registry's plugin factory with test-specific validators. The merge process
 * includes all plugins from the base registry first (unless disabled by the test factory), then
 * adds the essential test validators. This ensures existing registry behavior is preserved while
 * adding necessary test infrastructure.
 *
 * <p>The test factory can selectively disable specific plugins from the base registry by providing
 * matching disabled configurations, but typically it just adds essential validators without
 * conflicts.
 *
 * <p>For complete details on merge behavior, including duplicate resolution logic, plugin ordering,
 * and configuration handling, see the documentation on {@link
 * com.linkedin.metadata.aspect.plugins.PluginFactory#merge}.
 *
 * <h2>Usage Pattern</h2>
 *
 * <pre>{@code
 * // Enhance existing registry with test plugins
 * EntityRegistry enhancedRegistry = AspectTestUtils.enhanceRegistryWithTestPlugins(baseRegistry);
 *
 * // Use in OperationContext
 * OperationContext context = TestOperationContexts.Builder.builder()
 *     .entityRegistrySupplier(() -> enhancedRegistry)
 *     .buildSystemContext();
 * }</pre>
 *
 * @see PluginFactory
 * @see EntityRegistry
 * @see AspectPayloadValidator
 */
public class AspectTestUtils {

  /** Private constructor to prevent instantiation of utility class. */
  private AspectTestUtils() {
    // Utility class - no instances needed
  }

  /**
   * Creates a {@link PluginFactory} with essential aspect validators for testing.
   *
   * <p>This method programmatically creates and configures validator instances that are normally
   * discovered through Spring configuration in production. The factory includes:
   *
   * <ul>
   *   <li>{@link CreateIfNotExistsValidator} - Validates CREATE operations don't overwrite existing
   *       aspects
   *   <li>{@link ConditionalWriteValidator} - Validates conditional write headers and version
   *       checks
   * </ul>
   *
   * <p><strong>Design Note:</strong> This creates actual plugin instances rather than relying on
   * class loading or YAML configuration, making it suitable for isolated unit testing.
   *
   * @return A configured {@link PluginFactory} with essential test validators
   * @see #enhanceRegistryWithTestPlugins(EntityRegistry) for the recommended usage pattern
   */
  public static PluginFactory createTestPluginFactory() {
    List<AspectPayloadValidator> validators =
        List.of(
            new CreateIfNotExistsValidator().setConfig(createCreateIfNotExistsValidatorConfig()),
            new ConditionalWriteValidator().setConfig(createConditionalWriteValidatorConfig()));

    // Create PluginFactory with pre-built plugin instances
    return new PluginFactory(
        PluginConfiguration.EMPTY, // No config needed since we're providing instances
        Collections.emptyList(), // No custom class loaders
        validators,
        Collections.emptyList(), // No mutation hooks for basic testing
        Collections.emptyList(), // No MCL side effects for basic testing
        Collections.emptyList() // No MCP side effects for basic testing
        );
  }

  /**
   * Enhances an existing {@link EntityRegistry} with additional test plugins.
   *
   * <p>This is the <strong>recommended approach</strong> for adding test validators to an existing
   * registry without losing any existing configuration. The method:
   *
   * <ol>
   *   <li>Extracts the current plugin factory from the base registry
   *   <li>Creates a new factory with essential test validators
   *   <li>Merges both factories to preserve existing plugins
   *   <li>Returns a new registry that delegates to the original but uses the merged plugins
   * </ol>
   *
   * <p><strong>Benefits of this approach:</strong>
   *
   * <ul>
   *   <li>Backward compatible - preserves existing registry behavior
   *   <li>Additive - doesn't replace existing plugins, just adds test validators
   *   <li>Flexible - works with any EntityRegistry implementation
   * </ul>
   *
   * <p><strong>Typical usage:</strong>
   *
   * <pre>{@code
   * // In test base class constructor
   * EntityRegistry enhanced = AspectTestUtils.enhanceRegistryWithTestPlugins(
   *     TestOperationContexts.defaultEntityRegistry()
   * );
   *
   * // Use enhanced registry in operation context
   * OperationContext opContext = TestOperationContexts.systemContext(
   *     null, null, null, () -> enhanced, null, null, null, null, null, null
   * );
   * }</pre>
   *
   * @param baseRegistry The existing EntityRegistry to enhance
   * @return A new EntityRegistry that includes both original and test plugins
   * @throws NullPointerException if baseRegistry is null
   */
  public static EntityRegistry enhanceRegistryWithTestPlugins(EntityRegistry baseRegistry) {
    if (baseRegistry == null) {
      throw new NullPointerException("Base registry cannot be null");
    }

    PluginFactory basePlugins = baseRegistry.getPluginFactory();
    PluginFactory testPlugins = createTestPluginFactory();
    PluginFactory mergedPlugins = PluginFactory.merge(basePlugins, testPlugins, null);
    return new TestEntityRegistryWithPlugins(baseRegistry, mergedPlugins);
  }

  /**
   * Custom {@link EntityRegistry} implementation that delegates all operations to a wrapped
   * registry but overrides the plugin factory to use enhanced plugins.
   *
   * <p>This class follows the Decorator pattern, allowing us to enhance an existing registry with
   * additional plugins without modifying the original implementation. All EntityRegistry methods
   * are delegated to the wrapped instance except {@link #getPluginFactory()} which returns our
   * merged plugin factory.
   *
   * <p><strong>Thread Safety:</strong> This class is thread-safe if the delegate registry and
   * plugin factory are thread-safe (which they typically are in DataHub).
   */
  private static class TestEntityRegistryWithPlugins implements EntityRegistry {
    private final EntityRegistry delegate;
    private final PluginFactory pluginFactory;

    /**
     * Creates a new enhanced registry.
     *
     * @param delegate The original registry to wrap and delegate to
     * @param pluginFactory The enhanced plugin factory to use
     */
    public TestEntityRegistryWithPlugins(EntityRegistry delegate, PluginFactory pluginFactory) {
      this.delegate = delegate;
      this.pluginFactory = pluginFactory;
    }

    /**
     * Returns the enhanced plugin factory instead of the delegate's factory.
     *
     * @return The merged plugin factory containing both original and test plugins
     */
    @Override
    public PluginFactory getPluginFactory() {
      return pluginFactory;
    }

    // All remaining methods delegate to the wrapped registry to preserve original behavior

    @Override
    public com.linkedin.metadata.models.EntitySpec getEntitySpec(String entityName) {
      return delegate.getEntitySpec(entityName);
    }

    @Override
    public com.linkedin.metadata.models.EventSpec getEventSpec(String eventName) {
      return delegate.getEventSpec(eventName);
    }

    @Override
    public java.util.Map<String, com.linkedin.metadata.models.EntitySpec> getEntitySpecs() {
      return delegate.getEntitySpecs();
    }

    @Override
    public java.util.Map<String, com.linkedin.metadata.models.AspectSpec> getAspectSpecs() {
      return delegate.getAspectSpecs();
    }

    @Override
    public java.util.Map<String, com.linkedin.metadata.models.EventSpec> getEventSpecs() {
      return delegate.getEventSpecs();
    }

    @Override
    public com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine
        getAspectTemplateEngine() {
      return delegate.getAspectTemplateEngine();
    }
  }

  /**
   * Creates configuration for {@link CreateIfNotExistsValidator}.
   *
   * <p>This validator ensures that CREATE operations only succeed when the target aspect does not
   * already exist. It prevents accidental overwrites during CREATE operations and enforces proper
   * create semantics.
   *
   * <p><strong>Supported Operations:</strong>
   *
   * <ul>
   *   <li>CREATE - Single aspect creation
   *   <li>CREATE_ENTITY - Full entity creation with multiple aspects
   * </ul>
   *
   * <p><strong>Validation Logic:</strong> Fails with ValidationException if attempting to CREATE an
   * aspect that already exists for the given entity URN.
   *
   * @return Configuration for CreateIfNotExistsValidator
   */
  public static AspectPluginConfig createCreateIfNotExistsValidatorConfig() {
    return AspectPluginConfig.builder()
        .className(CreateIfNotExistsValidator.class.getName())
        .enabled(true)
        .supportedOperations(List.of("CREATE", "CREATE_ENTITY"))
        .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
        .build();
  }

  /**
   * Creates configuration for {@link ConditionalWriteValidator}.
   *
   * <p>This validator handles conditional write semantics by validating version headers and other
   * conditional write constraints. It ensures that operations with version requirements (e.g.,
   * "only update if current version is X") are properly validated.
   *
   * <p><strong>Supported Operations:</strong> All modification operations including CREATE, UPDATE,
   * UPSERT, PATCH, and DELETE.
   *
   * <p><strong>Validation Logic:</strong>
   *
   * <ul>
   *   <li>Validates If-Version-Match headers
   *   <li>Enforces version-based concurrency control
   *   <li>Prevents race conditions in concurrent updates
   * </ul>
   *
   * <p><strong>Example:</strong> If a client sends a request with "If-Version-Match: 5", this
   * validator ensures the current aspect version is actually 5 before allowing the write.
   *
   * @return Configuration for ConditionalWriteValidator
   */
  public static AspectPluginConfig createConditionalWriteValidatorConfig() {
    return AspectPluginConfig.builder()
        .className(ConditionalWriteValidator.class.getName())
        .enabled(true)
        .supportedOperations(
            List.of("CREATE", "CREATE_ENTITY", "DELETE", "UPSERT", "UPDATE", "PATCH"))
        .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
        .build();
  }
}
