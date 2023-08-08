package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.metadata.kafka.config.notification.IncidentNotificationGeneratorFactory;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;

@Slf4j
@Singleton
@Import({IncidentNotificationGeneratorFactory.class})
public class NotificationGeneratorHook implements MetadataChangeLogHook {

  private final List<MclNotificationGenerator> _notificationGenerators;
  private final boolean _isEnabled;

  @Autowired
  public NotificationGeneratorHook(@Nonnull final List<MclNotificationGenerator> notificationGenerators,
      @Nonnull @Value("${incidentNotification.enabled:true}") Boolean isEnabled) {
    _notificationGenerators = Objects.requireNonNull(notificationGenerators);
    _isEnabled = isEnabled;
  }

  @Override
  public void init() {
    // pass.
  }

  @Override
  public boolean isEnabled() {
    return _isEnabled;
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    for (MclNotificationGenerator notificationGenerator : _notificationGenerators) {
      CompletableFuture.runAsync(() -> {
        try {
          // Skip notification generation since this is an initial ingestion run to prevent slamming notifications on this run
          if (NotificationUtils.isFromInitialIngestionRun(event)) {
            log.info(
                String.format(
                    "Skipping NotificationGenerationHook since this is an initial ingestion run for this aspect. Run ID: %s",
                    event.getSystemMetadata() != null ? event.getSystemMetadata().getRunId() : null
                )
            );
          } else {
            notificationGenerator.generate(event);
          }
        } catch (Exception e) {
          log.error(String.format("Caught exception while invoking notification generator %s.",
              notificationGenerator.getClass().getCanonicalName()),
              e);
        }
      });
    }
  }
}