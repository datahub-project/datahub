package com.linkedin.metadata.dao.throttle;

import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@Builder
public class ThrottleControl {
  public static ThrottleControl NONE = ThrottleControl.builder().build();

  // call this after pause/sleep
  @Getter(AccessLevel.NONE)
  @Nullable
  Consumer<ThrottleEvent> callback;

  public boolean hasCallback() {
    return callback != null;
  }

  public void execute(ThrottleEvent throttleEvent) {
    if (callback != null) {
      callback.accept(throttleEvent);
    }
  }
}
