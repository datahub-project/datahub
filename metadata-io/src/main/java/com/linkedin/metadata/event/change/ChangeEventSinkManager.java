package com.linkedin.metadata.event.change;

import com.linkedin.platform.event.v1.EntityChangeEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * A manager of {@link ChangeEventSink}s.
 */
@Slf4j
public class ChangeEventSinkManager {

  private final List<ChangeEventSink> sinkRegistry;

  public ChangeEventSinkManager(@Nonnull final Collection<ChangeEventSink> sinks) {
    this.sinkRegistry = new ArrayList<>(sinks);
  }

  public CompletableFuture<Void> handle(@Nonnull final EntityChangeEvent event) {

    // TODO: Change to debug once we are ready.
    log.info(String.format("About to handle with sinks: %s, %s", this.sinkRegistry, event.toString()));

    // Send the change events to each registered sink.
    final List<CompletableFuture<Void>> sinkFutures = new ArrayList<>();
    for (final ChangeEventSink sink : this.sinkRegistry) {
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
