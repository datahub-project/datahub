package com.datahub.event.hook;

import com.datahub.notification.NotificationSinkManager;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.PlatformEvent;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** A {@link PlatformEventHook} that is responsible for processing {@link NotificationRequest}s. */
@Slf4j
@Component
public class NotificationSinkHook implements PlatformEventHook {

  private final NotificationSinkManager _sinkManager;

  private final boolean enabled;

  @Autowired
  public NotificationSinkHook(
      @Nonnull final NotificationSinkManager sinkManager,
      @Value("${notifications.enabled}") boolean enabled) {
    this._sinkManager = sinkManager;
    this.enabled = enabled;
  }

  @Override
  public void init() {
    log.info("Created notification sink hook");
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void invoke(@Nonnull OperationContext opContext, @Nonnull PlatformEvent event) {
    if (log.isDebugEnabled()) { // Avoid string formatting if not in debug mode
      log.debug(String.format("Received platform event %s", event.toString()));
    }
    if (Constants.NOTIFICATION_REQUEST_EVENT_NAME.equals(event.getName())) {
      final NotificationRequest notificationRequest =
          GenericRecordUtils.deserializePayload(
              event.getPayload().getValue(),
              event.getPayload().getContentType(),
              NotificationRequest.class);
      if (log.isDebugEnabled()) { // Avoid string formatting if not in debug mode
        log.debug(
            String.format("Received notification request %s", notificationRequest.toString()));
      }

      try {
        _sinkManager.handle(opContext, notificationRequest);
      } catch (Exception e) {
        // TODO: Determine what others means we have.
        log.error(
            String.format(
                "Caught exception while attempting to handle notification request %s",
                notificationRequest.toString()),
            e);
      }
    }
  }
}
