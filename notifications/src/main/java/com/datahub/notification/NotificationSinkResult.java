package com.datahub.notification;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class NotificationSinkResult {

  /**
   * The result of an attempt to send a notification.
   */
  public enum Result {
    /**
     * Notification was sent successfully.
     */
    SUCCESS,
    /**
     * Notification was not sent successfully.
     */
    FAILURE,
    /**
     * Notification was skipped.
     */
    SKIP
  }

  /**
   * Expected reasons that a notification may fail to send.
   */
  public enum Reason {
    /**
     * The sink does not support the recipient type that was provided.
     */
    UNSUPPORTED_RECIPIENT_TYPE,
    /**
     * Sink is disabled.
     */
    DISABLED_SINK,
    /**
     * An other reason for failure, which will be detailed in the 'message' field.
     */
    OTHER,
  }

  private final Result type;
  private final Reason reason;
  private final String message;

  public static NotificationSinkResult of(@Nonnull final Result type) {
    return NotificationSinkResult.of(type, null, null);
  }

  public static NotificationSinkResult of(@Nonnull final Result type, @Nullable Reason reason) {
    return NotificationSinkResult.of(type, reason, null);
  }

  public static NotificationSinkResult of(@Nonnull final Result type, @Nullable Reason reason, @Nullable String message) {
    return new NotificationSinkResult(type, reason, null);
  }
}
