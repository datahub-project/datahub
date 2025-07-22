package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemMetadataUtils {
  private static final String NO_OP_KEY = "isNoOp";

  private SystemMetadataUtils() {}

  public static SystemMetadata createDefaultSystemMetadata() {
    return generateSystemMetadataIfEmpty(null);
  }

  public static SystemMetadata createDefaultSystemMetadata(@Nullable String runId) {
    return generateSystemMetadataIfEmpty(
        new SystemMetadata()
            .setRunId(runId, SetMode.REMOVE_IF_NULL)
            .setLastObserved(System.currentTimeMillis()));
  }

  // Entry point for all MCPItem construction, if this assumption changes then logic for setting
  // aspectModified
  // needs propagation to retain auditability.
  public static SystemMetadata generateSystemMetadataIfEmpty(
      @Nullable SystemMetadata systemMetadata) {
    SystemMetadata result = systemMetadata == null ? new SystemMetadata() : systemMetadata;
    if (result.getRunId() == null) {
      result.setRunId(DEFAULT_RUN_ID);
    }
    if (!result.hasLastObserved() || result.getLastObserved() == 0) {
      result.setLastObserved(System.currentTimeMillis());
    }
    return result;
  }

  public static SystemMetadata setAspectModified(
      @Nonnull SystemMetadata systemMetadata, @Nullable AuditStamp auditStamp) {
    if (auditStamp != null && !SYSTEM_ACTOR.equals(auditStamp.getActor().toString())) {
      systemMetadata.setAspectModified(auditStamp);
    }
    return systemMetadata;
  }

  public static SystemMetadata parseSystemMetadata(String jsonSystemMetadata) {
    if (jsonSystemMetadata == null || jsonSystemMetadata.equals("")) {
      return createDefaultSystemMetadata();
    }
    return RecordUtils.toRecordTemplate(SystemMetadata.class, jsonSystemMetadata);
  }

  public static boolean isNoOp(@Nullable SystemMetadata systemMetadata) {
    if (systemMetadata != null && systemMetadata.hasProperties()) {
      return Boolean.parseBoolean(systemMetadata.getProperties().getOrDefault(NO_OP_KEY, "false"));
    }

    return false;
  }

  @Nullable
  public static SystemMetadata setNoOp(@Nullable SystemMetadata systemMetadata, boolean isNoOp) {
    if (systemMetadata != null) {
      if (!systemMetadata.hasProperties()) {
        systemMetadata.setProperties(new StringMap());
      }
      systemMetadata.getProperties().put(NO_OP_KEY, String.valueOf(isNoOp));
    }
    return systemMetadata;
  }
}
