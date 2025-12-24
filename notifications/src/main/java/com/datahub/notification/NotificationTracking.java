package com.datahub.notification;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NotificationTracking {

  public static final String NOTIFICATION_QUERY_PARAM_TYPE = "notification_type";
  public static final String NOTIFICATION_QUERY_PARAM_ID = "notification_id";
  public static final String NOTIFICATION_QUERY_PARAM_CHANNEL = "notification_channel";

  public enum NotificationChannel {
    SLACK("slack");

    private final String value;

    NotificationChannel(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  public enum NotificationType {
    ASSERTION("assertion"),
    INCIDENT("incident");

    private final String value;

    NotificationType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }
}
