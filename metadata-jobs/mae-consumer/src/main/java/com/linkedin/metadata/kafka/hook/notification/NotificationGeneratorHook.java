package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
@Singleton
public class NotificationGeneratorHook implements MetadataChangeLogHook {

  private final List<MclNotificationGenerator> _notificationGenerators;
  private final boolean _isEnabled;

  @Autowired
  public NotificationGeneratorHook(
      @Nonnull final List<MclNotificationGenerator> notificationGenerators,
      @Nonnull @Value("${notifications.enabled:true}") Boolean isEnabled) {
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
    List<CompletableFuture<Void>> futures =
        _notificationGenerators.stream()
            .map(
                notificationGenerator ->
                    CompletableFuture.runAsync(
                        () -> {
                          try {
                            // Skip notification generation since this is an initial ingestion run
                            // to prevent slamming notifications on this run
                            if (!NotificationUtils.isEligibleForNotificationGeneration(event)) {
                              log.debug(
                                  String.format(
                                      "Skipping NotificationGenerationHook since this aspect is not eligible for generation. aspect: %s, Run ID: %s",
                                      event.getAspectName(),
                                      event.getSystemMetadata() != null
                                          ? event.getSystemMetadata().getRunId()
                                          : null));
                            } else {
                              notificationGenerator.generate(event);
                            }
                          } catch (Exception e) {
                            log.error(
                                String.format(
                                    "Caught exception while invoking notification generator %s.",
                                    notificationGenerator.getClass().getCanonicalName()),
                                e);
                          }
                        }))
            .collect(Collectors.toList());

    futures.forEach(CompletableFuture::join);
  }
}
