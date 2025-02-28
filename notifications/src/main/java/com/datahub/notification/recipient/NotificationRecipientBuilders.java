package com.datahub.notification.recipient;

import com.linkedin.event.notification.NotificationSinkType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;

public class NotificationRecipientBuilders {

  private final Map<NotificationSinkType, NotificationRecipientBuilder> builders;

  public NotificationRecipientBuilders(
      @Nonnull final Map<NotificationSinkType, NotificationRecipientBuilder> builders) {
    this.builders = Objects.requireNonNull(builders, "builders must not be null");
  }

  public NotificationRecipientBuilder getBuilder(@Nonnull final NotificationSinkType type) {
    return builders.get(type);
  }

  @Nonnull
  public List<NotificationRecipientBuilder> listBuilders() {
    return new ArrayList<>(builders.values());
  }
}
