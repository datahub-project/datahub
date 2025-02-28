package com.datahub.notification;

import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.SetMode;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * A manager of notification sinks. This component is responsible for more semantic aspects of
 * notification sending, including performing user lookups, [TODO] verifying a user's preferences,
 * and routing incoming {@link com.linkedin.event.notification.NotificationRequest}s to eligible
 * sinks.
 *
 * <p>This class is responsible for performing any recipient preferences checks and invoking
 * specific notification sinks to handle a message send.
 *
 * <p>Also responsible for initializing a notification sink.
 */
@Slf4j
public class NotificationSinkManager {

  /** The mode of operation for the Notification Sink Manager. */
  public enum NotificationManagerMode {
    /** Notification manager is enabled. */
    ENABLED,
    /** Notification manager is disabled. */
    DISABLED
  }

  private final NotificationManagerMode mode;
  private final List<NotificationSink> sinkRegistry;

  public NotificationSinkManager(@Nonnull final Collection<NotificationSink> sinks) {
    this(NotificationManagerMode.ENABLED, sinks);
  }

  public NotificationSinkManager(
      @Nonnull NotificationManagerMode mode, @Nonnull final Collection<NotificationSink> sinks) {
    this.mode = mode;
    this.sinkRegistry = new ArrayList<>(sinks);
  }

  public CompletableFuture<Void> handle(
      @Nonnull OperationContext opContext, @Nonnull final NotificationRequest request) {
    if (NotificationManagerMode.DISABLED.equals(this.mode)) {
      log.debug("NotificationSinkManager is disabled. Skipping sending notification...");
      return CompletableFuture.completedFuture(null);
    }

    log.info("Handling notification request: {}, sinks: {}", request.toString(), this.sinkRegistry);

    // 1. Validate & extract the requested template and corresponding arguments.
    final NotificationTemplateType template =
        validateTemplate(
            request.getMessage().getTemplate().toString(), request.getMessage().getParameters());

    // 2. Identify the sinks capable of handling the template.
    final List<NotificationSink> eligibleSinks = getEligibleSinks(template, request);

    // 3. Send the messages via each sink.
    final List<CompletableFuture<Void>> notificationFutures = new ArrayList<>();
    for (final NotificationSink sink : eligibleSinks) {

      log.info(
          "Sinking notification request to sink with type {}", sink.getClass().getCanonicalName());

      // Run each sink asynchronously.
      notificationFutures.add(
          CompletableFuture.runAsync(
              () -> {
                try {
                  final NotificationRequest finalRequest =
                      filterForEligibleRecipients(request, sink);
                  if (finalRequest.getRecipients().isEmpty()) {
                    log.info(
                        String.format(
                            "Skipping notification request for sink %s. No eligible recipients found.",
                            sink.getClass()));
                    return;
                  }
                  sink.send(opContext, finalRequest, new NotificationContext());
                } catch (Exception e) {
                  log.error(
                      String.format(
                          "Caught exception while attempting to sink notification request to sink %s. template: %s, params: %s, recipients: %s",
                          sink.getClass(),
                          request.getMessage().getTemplate(),
                          request.getMessage().getParameters(),
                          request.getRecipients()),
                      e);
                }
              }));
    }
    return CompletableFuture.allOf(
        notificationFutures.toArray(new CompletableFuture[eligibleSinks.size()]));
  }

  /**
   * Validate an incoming Notification Template name against a set of known types.
   *
   * @param template the template to validate
   * @return the corresponding {@link NotificationTemplateType}.
   */
  private NotificationTemplateType validateTemplate(
      @Nonnull final String template, @Nullable final Map<String, String> parameters) {
    NotificationTemplateType templateType;
    try {
      templateType = NotificationTemplateType.valueOf(template);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(
          String.format(
              "Failed to validate Notification Template Type. Unsupported template with name %s provided.",
              template));
    }
    if (templateType.getRequiredParameters().size() > 0) {
      if (parameters == null) {
        throw new RuntimeException(
            String.format("Found null parameters for template with name %s", template));
      }
      validateRequiredParameters(templateType, parameters);
    }
    return templateType;
  }

  @Nonnull
  private NotificationRequest filterForEligibleRecipients(
      @Nonnull final NotificationRequest request, @Nonnull final NotificationSink sink) {
    final List<NotificationRecipient> eligibleRecipients =
        request.getRecipients().stream()
            .filter(recipient -> sink.recipientTypes().contains(recipient.getType()))
            .collect(Collectors.toList());
    return new NotificationRequest()
        .setMessage(request.getMessage())
        .setRecipients(new NotificationRecipientArray(eligibleRecipients))
        .setSinks(request.getSinks(GetMode.NULL), SetMode.IGNORE_NULL);
  }

  private void validateRequiredParameters(
      @Nonnull final NotificationTemplateType template,
      @Nonnull final Map<String, String> parameters) {
    for (final String parameter : template.getRequiredParameters()) {
      if (!parameters.containsKey(parameter)) {
        throw new RuntimeException(
            String.format(
                "Failed to validate notification request: Notification template %s is missing required parameter %s",
                template.toString(), parameter));
      }
    }
  }

  private List<NotificationSink> getEligibleSinks(
      final NotificationTemplateType type, final NotificationRequest request) {
    final List<NotificationSink> eligibleTemplateSinks = getEligibleSinksFromTemplate(type);

    // If the request has requested specific sinks, direct only to those.
    if (request.getSinks() != null) {
      final Set<NotificationSinkType> requestedSinkTypes =
          request.getSinks().stream()
              .map(com.linkedin.event.notification.NotificationSink::getType)
              .collect(Collectors.toSet());
      return eligibleTemplateSinks.stream()
          .filter(sink -> requestedSinkTypes.contains(sink.type()))
          .collect(Collectors.toList());
    }
    // Otherwise, return all eligible sinks
    return eligibleTemplateSinks;
  }

  private List<NotificationSink> getEligibleSinksFromTemplate(
      final NotificationTemplateType template) {
    return this.sinkRegistry.stream()
        .filter(sink -> sink.templates().contains(template))
        .collect(Collectors.toList());
  }
}
