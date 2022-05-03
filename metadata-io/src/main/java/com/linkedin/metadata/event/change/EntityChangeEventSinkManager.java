package com.linkedin.metadata.event.change;

import com.datahub.util.RecordUtils;
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
    // Send the change events to each registered sink.
    final List<CompletableFuture<Void>> sinkFutures = new ArrayList<>();
    for (final EntityChangeEventSink sink : this.sinkRegistry) {
      // Run each sink asynchronously.
      sinkFutures.add(CompletableFuture.runAsync(() -> {
        try {
          log.debug(String.format("Sinking event to sink with type %s", sink.getClass().getCanonicalName()));
          sink.sink(event);
        } catch (Exception e) {
          // This is very bad. It means that we could not sync events to the external destination.
          // TODO: Better handling via failed events log. For now, simply print entire event so we know which have failed.
          log.error(String.format("Caught exception while attempting to sink change event to sink %s. event: %s", sink.getClass(), RecordUtils
              .toJsonString(event)), e);
        }
      }));
    }
    return CompletableFuture.allOf(sinkFutures.toArray(new CompletableFuture[this.sinkRegistry.size()]));
  }
}
