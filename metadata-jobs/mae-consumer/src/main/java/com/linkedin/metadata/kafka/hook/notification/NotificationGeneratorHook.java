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
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Singleton
@Import({IncidentNotificationGeneratorFactory.class})
public class NotificationGeneratorHook implements MetadataChangeLogHook {

  private final List<MclNotificationGenerator> _notificationGenerators;

  @Autowired
  public NotificationGeneratorHook(@Nonnull final List<MclNotificationGenerator> notificationGenerators) {
    _notificationGenerators = Objects.requireNonNull(notificationGenerators);
  }

  @Override
  public void init() {
    // pass.
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    for (MclNotificationGenerator notificationGenerator : _notificationGenerators) {
      CompletableFuture.runAsync(() -> {
        try {
          notificationGenerator.generate(event);
        } catch (Exception e) {
          log.error(String.format("Caught exception while invoking notification generator %s.",
              notificationGenerator.getClass().getCanonicalName()),
              e);
        }
      });
    }
  }
}