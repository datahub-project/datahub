package com.linkedin.metadata.resources.platform;

import com.linkedin.entity.Entity;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import io.opentelemetry.extension.annotations.WithSpan;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

/**
 * DataHub Platform Actions
 */
@Slf4j
@RestLiCollection(name = "platform", namespace = "com.linkedin.platform")
public class PlatformResource extends CollectionResourceTaskTemplate<String, Entity> {

  private static final String ACTION_PRODUCE_PLATFORM_EVENT = "producePlatformEvent";

  @Inject
  @Named("kafkaEventProducer")
  private EventProducer _eventProducer;

  @Action(name = ACTION_PRODUCE_PLATFORM_EVENT)
  @Nonnull
  @WithSpan
  public Task<Void> producePlatformEvent(
      @ActionParam("name") @Nonnull String eventName,
      @ActionParam("key") @Optional String key,
      @ActionParam("event") @Nonnull PlatformEvent event) {
    log.info(String.format("Emitting platform event. name: %s, key: %s", eventName, key));
    return RestliUtil.toTask(() -> {
      _eventProducer.producePlatformEvent(eventName, key, event);
      return null;
    });
  }
}
