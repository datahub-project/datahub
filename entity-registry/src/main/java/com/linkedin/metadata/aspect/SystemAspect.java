package com.linkedin.metadata.aspect;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import java.sql.Timestamp;
import javax.annotation.Nonnull;

/**
 * An aspect along with system metadata and creation timestamp. Represents an aspect as stored in
 * primary storage.
 */
public interface SystemAspect extends ReadItem {
  long getVersion();

  Timestamp getCreatedOn();

  String getCreatedBy();

  @Nonnull
  default AuditStamp getAuditStamp() {
    return new AuditStamp()
        .setActor(UrnUtils.getUrn(getCreatedBy()))
        .setTime(getCreatedOn().getTime());
  }
}
