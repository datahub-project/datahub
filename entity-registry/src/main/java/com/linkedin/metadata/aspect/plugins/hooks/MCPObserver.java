package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Observe MCPs before the database transaction without producing additional MCPs or MCLs. Intended
 * for external actions like metrics collection that need pre-commit visibility.
 */
@Slf4j
public abstract class MCPObserver extends PluginSpec {

  /**
   * Filters items through {@code shouldApply()} and delegates to {@link #observeMCPs}.
   *
   * <p>Observers are pre-transaction, side-effect-only (metrics/logs/external actions). They must
   * never fail the ingest path. We catch and log any throwable here so a buggy observer (or a
   * cast/NPE against a malformed item in the batch) cannot crash GMS ingestion. The full stack is
   * logged at WARN so it surfaces even when ERROR-level logging is filtered downstream.
   */
  public final void apply(
      Collection<? extends BatchItem> items, @Nonnull RetrieverContext retrieverContext) {
    try {
      observeMCPs(
          items.stream()
              .filter(
                  item -> shouldApply(item.getChangeType(), item.getUrn(), item.getAspectName()))
              .collect(Collectors.toList()),
          retrieverContext);
    } catch (VirtualMachineError e) {
      // Never swallow JVM-fatal errors (OOM, StackOverflow, InternalError, UnknownError).
      throw e;
    } catch (Throwable t) {
      log.warn(
          "MCPObserver {} failed; ingest continuing. batch_size={}",
          getClass().getName(),
          items == null ? -1 : items.size(),
          t);
    }
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
