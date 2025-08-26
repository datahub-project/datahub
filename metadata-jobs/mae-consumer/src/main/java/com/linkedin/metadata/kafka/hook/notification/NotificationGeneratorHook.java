package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NotificationGeneratorHook implements MetadataChangeLogHook {

  private final List<MclNotificationGenerator> mclNotificationGenerators;
  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;

  public NotificationGeneratorHook(
      @Nonnull final List<MclNotificationGenerator> notificationGenerators,
      @Nonnull Boolean isEnabled,
      @Nonnull String consumerGroupSuffix) {
    mclNotificationGenerators = Objects.requireNonNull(notificationGenerators);
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @Override
  public NotificationGeneratorHook init(@Nonnull OperationContext systemOperationContext) {
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    List<CompletableFuture<Void>> futures =
        mclNotificationGenerators.stream()
            .map(
                notificationGenerator ->
                    CompletableFuture.runAsync(
                        () -> {
                          try {
                            // Skip notification generation since this is an initial ingestion run
                            // to prevent slamming notifications on this run
                            if (!NotificationUtils.isEligibleForNotificationGeneration(event)) {
                              log.debug(
                                  "Skipping NotificationGenerationHook since this aspect is not eligible for generation. aspect: {}, Run ID: {}",
                                  event.getAspectName(),
                                  event.getSystemMetadata() != null
                                      ? event.getSystemMetadata().getRunId()
                                      : null);
                            } else {
                              notificationGenerator.generate(event);
                            }
                          } catch (Exception e) {
                            log.error(
                                "Caught exception while invoking notification generator {}",
                                notificationGenerator.getClass().getCanonicalName(),
                                e);
                          }
                        }))
            .collect(Collectors.toList());

    futures.forEach(CompletableFuture::join);
  }
}
