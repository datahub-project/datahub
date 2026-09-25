package com.linkedin.metadata.usage.registry.operations;

import javax.annotation.Nonnull;

/** Activity allowlist flags derived from {@link ActivityClass} at record time. */
public record ActivitySnapshot(
    boolean activityAllowlist, boolean readerAllowlist, boolean writerAllowlist) {

  @Nonnull
  public static ActivitySnapshot fromActivityClass(@Nonnull ActivityClass activityClass) {
    boolean readOrOperation =
        activityClass == ActivityClass.READ || activityClass == ActivityClass.OPERATION;
    boolean write = activityClass == ActivityClass.WRITE;
    boolean anyActivity =
        activityClass == ActivityClass.READ
            || activityClass == ActivityClass.WRITE
            || activityClass == ActivityClass.OPERATION;
    return new ActivitySnapshot(anyActivity, readOrOperation, write);
  }
}
