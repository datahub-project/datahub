package com.datahub.event.hook.change;

import com.datahub.event.hook.PlatformEventHook;
import com.linkedin.gms.factory.change.EntityChangeEventSinkManagerFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.change.EntityChangeEventSinkManager;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;


/**
 * A {@link PlatformEventHook} that is responsible for sinking {@link com.linkedin.platform.event.v1.EntityChangeEvent}s
 * to external platforms (like AWS Event Bridge)
 */
@Slf4j
@Component
@Import({EntityChangeEventSinkManagerFactory.class})
public class EntityChangeEventSinkHook implements PlatformEventHook {

  private final EntityChangeEventSinkManager _sinkManager;

  @Autowired
  public EntityChangeEventSinkHook(@Nonnull final EntityChangeEventSinkManager sinkManager) {
    _sinkManager = sinkManager;
  }

  @Override
  public void init() {
    // pass.
  }

  @Override
  public void invoke(@Nonnull PlatformEvent event) {
    if (Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME.equals(event.getName())) {
      final EntityChangeEvent changeEvent = GenericRecordUtils.deserializePayload(
          event.getPayload().getValue(),
          event.getPayload().getContentType(),
          EntityChangeEvent.class
      );
      log.debug(String.format("Received entity change event %s", event));
      // Errors are caught at the nested task level.
      _sinkManager.handle(changeEvent).join();
    }
  }
}
