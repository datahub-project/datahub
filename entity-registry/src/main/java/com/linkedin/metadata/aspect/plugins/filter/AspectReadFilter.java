package com.linkedin.metadata.aspect.plugins.filter;

import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import javax.annotation.Nonnull;

/**
 * Drops unauthorized aspects at DAO egress. Unlike {@code MutationHook}, filters deny rather than
 * transform.
 */
public abstract class AspectReadFilter extends PluginSpec {

  private static final String READ = "READ";
  private static final String EXISTS = "EXISTS";

  public boolean shouldApply(
      @Nonnull final ReadIntent intent,
      @Nonnull final String entityName,
      @Nonnull final String aspectName) {
    return getConfig().isEnabled()
        && isReadIntentSupported(intent)
        && isEntityAspectSupported(entityName, aspectName);
  }

  /**
   * @return {@code true} if the aspect row may be returned to the caller
   */
  public abstract boolean isAllowed(
      @Nonnull AspectReadContext context, @Nonnull EntityRegistry entityRegistry);

  private boolean isReadIntentSupported(@Nonnull final ReadIntent intent) {
    return getConfig().getSupportedOperations().stream()
        .anyMatch(
            supported ->
                WILDCARD.equals(supported)
                    || READ.equalsIgnoreCase(supported) && intent == ReadIntent.READ
                    || EXISTS.equalsIgnoreCase(supported) && intent == ReadIntent.EXISTS);
  }
}
