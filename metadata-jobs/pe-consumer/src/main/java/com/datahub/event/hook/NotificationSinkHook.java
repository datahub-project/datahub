package com.datahub.event.hook;

import com.linkedin.gms.factory.notifications.NotificationSinkManagerFactory;
import com.datahub.notification.NotificationSinkManager;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.PlatformEvent;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;


/**
 * A {@link PlatformEventHook} that is responsible for processing {@link NotificationRequest}s.
 */
@Slf4j
@Component
@Import({NotificationSinkManagerFactory.class})
public class NotificationSinkHook implements PlatformEventHook {

  private final NotificationSinkManager _sinkManager;

  @Autowired
  public NotificationSinkHook(@Nonnull final NotificationSinkManager sinkManager) {
    _sinkManager = sinkManager;
  }

  @Override
  public void init() {
  }

  @Override
  public void invoke(@Nonnull PlatformEvent event) {
    log.info(String.format("Received platform event %s", event.toString()));
    if (Constants.NOTIFICATION_REQUEST_EVENT_NAME.equals(event.getName())) {
      final NotificationRequest notificationRequest = GenericRecordUtils.deserializePayload(
          event.getPayload().getValue(),
          event.getPayload().getContentType(),
          NotificationRequest.class
      );
      log.info(String.format("Received notification request %s", notificationRequest.toString()));

      try {
        _sinkManager.handle(notificationRequest);
      } catch (Exception e) {
        // TODO: Determine what others means we have.
        log.error(String.format("Caught exception while attempting to handle notification request %s", notificationRequest.toString()), e);
      }
    }
  }
}
