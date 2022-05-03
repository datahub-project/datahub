package com.datahub.event.hook.change;

import com.datahub.event.hook.PlatformEventHook;
import com.linkedin.gms.factory.change.ChangeEventSinkFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.change.ChangeEventSinkManager;
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
@Import({ChangeEventSinkFactory.class})
public class ChangeEventSinkHook implements PlatformEventHook {

  private final ChangeEventSinkManager _sinkManager;

  @Autowired
  public ChangeEventSinkHook(@Nonnull final ChangeEventSinkManager sinkManager) {
    _sinkManager = sinkManager;
  }

  @Override
  public void init() {
    // pass.
  }

  @Override
  public void invoke(@Nonnull PlatformEvent event) {
    log.info(String.format("Received platform event %s", event.toString()));
    if (Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME.equals(event.getName())) {
      final EntityChangeEvent changeEvent = GenericRecordUtils.deserializePayload(
          event.getPayload().getValue(),
          event.getPayload().getContentType(),
          EntityChangeEvent.class
      );
      log.debug(String.format("Received change event %s", event));
      try {
        _sinkManager.handle(changeEvent);
      } catch (Exception e) {
        // TODO: Determine what others means we have.
        // This is a serious issue. Think about logging this to a file.
        log.error(String.format("Caught exception while attempting to mirror change event: %s", event.toString()), e);
      }
    }
  }
}
