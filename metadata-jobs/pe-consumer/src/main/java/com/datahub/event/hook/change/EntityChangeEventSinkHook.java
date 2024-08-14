package com.datahub.event.hook.change;

import com.datahub.event.hook.PlatformEventHook;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.change.EntityChangeEventSinkManager;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * A {@link PlatformEventHook} that is responsible for sinking {@link
 * com.linkedin.platform.event.v1.EntityChangeEvent}s to external platforms (like AWS Event Bridge)
 */
@Slf4j
@Component
public class EntityChangeEventSinkHook implements PlatformEventHook {

  private final EntityChangeEventSinkManager _sinkManager;

  private final boolean enabled;

  @Autowired
  public EntityChangeEventSinkHook(
      @Nonnull final EntityChangeEventSinkManager sinkManager,
      @Value("${entityChangeEvents.enabled}") boolean enabled) {
    this._sinkManager = sinkManager;
    this.enabled = enabled;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void invoke(@Nonnull OperationContext opContext, @Nonnull PlatformEvent event) {
    if (Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME.equals(event.getName())) {
      final EntityChangeEvent changeEvent =
          GenericRecordUtils.deserializePayload(
              event.getPayload().getValue(),
              event.getPayload().getContentType(),
              EntityChangeEvent.class);
      log.debug(String.format("Received entity change event %s", event));
      // Errors are caught at the nested task level.
      _sinkManager.handle(changeEvent).join();
    }
  }
}
