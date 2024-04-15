package com.linkedin.metadata.utils;

import com.linkedin.metadata.Constants;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemMetadataUtils {

  private SystemMetadataUtils() {}

  public static SystemMetadata createDefaultSystemMetadata() {
    return new SystemMetadata()
        .setRunId(Constants.DEFAULT_RUN_ID)
        .setLastObserved(System.currentTimeMillis());
  }

  public static SystemMetadata generateSystemMetadataIfEmpty(
      @Nullable SystemMetadata systemMetadata) {
    return systemMetadata == null ? createDefaultSystemMetadata() : systemMetadata;
  }
}
