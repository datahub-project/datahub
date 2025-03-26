package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nullable;

public class NotificationRecipientsGeneratorExtraContext {

  private Urn _modifierUrn;
  private RecordTemplate _originalAspect;

  public NotificationRecipientsGeneratorExtraContext setModifierUrn(@Nullable Urn modifierUrn) {
    _modifierUrn = modifierUrn;
    return this;
  }

  public NotificationRecipientsGeneratorExtraContext setOriginalAspect(
      @Nullable RecordTemplate originalAspect) {
    _originalAspect = originalAspect;
    return this;
  }

  @Nullable
  public Urn getModifierUrn() {
    return _modifierUrn;
  }

  @Nullable
  public RecordTemplate getOriginalAspect() {
    return _originalAspect;
  }
}
