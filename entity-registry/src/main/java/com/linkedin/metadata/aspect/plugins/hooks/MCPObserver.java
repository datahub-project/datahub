package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Observe MCPs before the database transaction without producing additional MCPs or MCLs. Intended
 * for external actions like metrics collection that need pre-commit visibility.
 */
public abstract class MCPObserver extends PluginSpec {

  /**
   * Filters items through {@code shouldApply()} and delegates to {@link #observeMCPs}.
   *
   * @param items incoming batch items
   * @param retrieverContext accessors for aspect and graph data
   */
  public final void apply(
      Collection<? extends BatchItem> items, @Nonnull RetrieverContext retrieverContext) {
    observeMCPs(
        items.stream()
            .filter(item -> shouldApply(item.getChangeType(), item.getUrn(), item.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  /**
   * Observe the filtered MCPs. Implementations must not produce side effects that write MCPs or
   * MCLs.
   *
   * @param items filtered batch items that passed {@code shouldApply()}
   * @param retrieverContext accessors for aspect and graph data
   */
  protected abstract void observeMCPs(
      Collection<? extends BatchItem> items, @Nonnull RetrieverContext retrieverContext);
}
