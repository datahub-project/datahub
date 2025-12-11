/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
