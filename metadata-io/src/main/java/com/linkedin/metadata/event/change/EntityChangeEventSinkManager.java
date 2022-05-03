package com.linkedin.metadata.event.change;

import com.linkedin.platform.event.v1.EntityChangeEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * A manager of {@link EntityChangeEventSink}s.
 */
@Slf4j
public class EntityChangeEventSinkManager {

  private final List<EntityChangeEventSink> sinkRegistry;

  public EntityChangeEventSinkManager(@Nonnull final Collection<EntityChangeEventSink> sinks) {
    this.sinkRegistry = new ArrayList<>(sinks);
  }

  public CompletableFuture<Void> handle(@Nonnull final EntityChangeEvent event) {

    log.debug(String.format("About to handle with sinks: %s, %s", this.sinkRegistry, event.toString()));

    // Send the change events to each registered sink.
    final List<CompletableFuture<Void>> sinkFutures = new ArrayList<>();
    for (final EntityChangeEventSink sink : this.sinkRegistry) {
      // Run each sink asynchronously.
      sinkFutures.add(CompletableFuture.runAsync(() -> {
        try {
          sink.sink(event);
        } catch (Exception e) {
          log.error(String.format("Caught exception while attempting to sink change event to sink %s.", sink.getClass()), e);
        }
      }));
    }
    return CompletableFuture.allOf(sinkFutures.toArray(new CompletableFuture[this.sinkRegistry.size()]));
  }
}
