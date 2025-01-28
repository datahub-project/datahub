package com.linkedin.metadata.aspect;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.NotImplementedException;

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

  /**
   * If aspect version exists in system metadata, return it
   *
   * @return version of the aspect
   */
  default Optional<Long> getSystemMetadataVersion() {
    return Optional.ofNullable(getSystemMetadata())
        .filter(SystemMetadata::hasVersion)
        .map(SystemMetadata::getVersion)
        .map(Long::parseLong);
  }

  @Override
  default void setSystemMetadata(@Nonnull SystemMetadata systemMetadata) {
    throw new NotImplementedException();
  }
}
