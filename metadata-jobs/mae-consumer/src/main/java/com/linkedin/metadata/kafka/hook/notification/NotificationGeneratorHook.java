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

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;

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
          // if there is no previous aspect and this MCL has a run ID not equal to DEFAULT_RUN_ID (meaning this is coming from ingestion), skip notification generation since this is an initial ingestion run.
          if (event.getPreviousAspectValue() == null && event.hasSystemMetadata() && event.getSystemMetadata().hasRunId() && !event.getSystemMetadata().getRunId().equals(DEFAULT_RUN_ID)) {
            notificationGenerator.generate(event);
          } else {
            log.info(
                String.format(
                    "Skipping NotificationGenerationHook since this is an initial ingestion run for this aspect. Run ID: %s, Aspect: %s",
                    event.getSystemMetadata() != null ? event.getSystemMetadata().getRunId() : null,
                    event.getAspect() != null ? event.getAspect().toString() : null
                )
            );
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