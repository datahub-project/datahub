package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nullable;

public class NotificationRecipientsGeneratorExtraContext {

  private Urn _modifierUrn;

  public NotificationRecipientsGeneratorExtraContext setModifierUrn(@Nullable Urn modifierUrn) {
    _modifierUrn = modifierUrn;
    return this;
  }

  @Nullable
  public Urn getModifierUrn() {
    return _modifierUrn;
  }
}
