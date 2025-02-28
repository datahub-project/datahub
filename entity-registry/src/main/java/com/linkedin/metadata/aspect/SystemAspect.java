package com.linkedin.metadata.aspect;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An aspect along with system metadata and creation timestamp. Represents an aspect as stored in
 * primary storage.
 */
public interface SystemAspect extends ReadItem {

  long getVersion();

  Timestamp getCreatedOn();

  String getCreatedBy();

  /**
   * If we're tracking the database state with an ORM model, or we need to get the initial database
   * aspect before batch mutations.
   *
   * @return The database aspect if it already exists in the database.
   */
  @Nonnull
  Optional<SystemAspect> getDatabaseAspect();

  /**
   * Return this aspect for update or insertion into the database with version 0
   *
   * @return entity aspect after potential mutation for version 0
   */
  @Nonnull
  default EntityAspect asLatest() {
    return withVersion(ASPECT_LATEST_VERSION);
  }

  /**
   * Return this aspect for insertion into the database with the given version
   *
   * @param version the version column
   * @return entity aspect after potential mutation for the specified version
   */
  @Nonnull
  EntityAspect withVersion(long version);

  @Nonnull
  SystemAspect copy();

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

  @Nonnull
  SystemAspect setSystemMetadata(@Nullable SystemMetadata systemMetadata);

  @Nonnull
  SystemAspect setRecordTemplate(@Nonnull RecordTemplate recordTemplate);

  @Nonnull
  SystemAspect setAuditStamp(@Nonnull AuditStamp auditStamp);

  @Nonnull
  SystemAspect setDatabaseAspect(@Nonnull SystemAspect databaseAspect);
}
