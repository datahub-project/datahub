package com.linkedin.metadata.utils;

import com.linkedin.metadata.Constants;
import com.linkedin.mxe.SystemMetadata;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemMetadataUtils {

  private SystemMetadataUtils() {}

  public static SystemMetadata createDefaultSystemMetadata() {
    return new SystemMetadata()
        .setRunId(Constants.DEFAULT_RUN_ID)
        .setLastObserved(System.currentTimeMillis());
  }
}
