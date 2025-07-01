package com.linkedin.metadata.aspect;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;

/** Delegate to restli class */
@Builder(toBuilder = true)
@AllArgsConstructor
public class EnvelopedSystemAspect implements SystemAspect {

  public static SystemAspect of(
      @Nonnull Urn urn, @Nonnull EnvelopedAspect envelopedAspect, @Nonnull EntitySpec entitySpec) {
    return new EnvelopedSystemAspect(urn, envelopedAspect, entitySpec);
  }

  @Getter @Nonnull private final Urn urn;
  @Nonnull private final EnvelopedAspect envelopedAspect;
  @Getter @Nonnull private final EntitySpec entitySpec;
  @Getter @Nonnull private final AspectSpec aspectSpec;

  public EnvelopedSystemAspect(
      @Nonnull Urn urn, @Nonnull EnvelopedAspect envelopedAspect, @Nonnull EntitySpec entitySpec) {
    this.urn = urn;
    this.envelopedAspect = envelopedAspect;
    this.entitySpec = entitySpec;
    this.aspectSpec = this.entitySpec.getAspectSpec(envelopedAspect.getName());
  }

  @Nullable
  @Override
  public RecordTemplate getRecordTemplate() {
    return envelopedAspect.getValue();
  }

  @Nullable
  @Override
  public SystemMetadata getSystemMetadata() {
    return envelopedAspect.getSystemMetadata();
  }

  @Override
  public long getVersion() {
    return envelopedAspect.getVersion();
  }

  @Override
  public Timestamp getCreatedOn() {
    return Timestamp.from(Instant.ofEpochMilli(envelopedAspect.getCreated().getTime()));
  }

  @Override
  public String getCreatedBy() {
    return envelopedAspect.getCreated().getActor().toString();
  }

  @Nonnull
  @Override
  public SystemAspect copy() {
    try {
      return new EnvelopedSystemAspect(
          urn, new EnvelopedAspect(envelopedAspect.copy().data()), entitySpec, aspectSpec);
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * These methods are not implemented as this SystemAspect is not intended for write
   */
  @Override
  @Nonnull
  public EntityAspect asLatest() {
    throw new NotImplementedException();
  }

  @Override
  @Nonnull
  public EntityAspect withVersion(long version) {
    throw new NotImplementedException();
  }

  @Nonnull
  @Override
  public SystemAspect setSystemMetadata(@Nullable SystemMetadata systemMetadata) {
    throw new NotImplementedException();
  }

  @Nonnull
  @Override
  public SystemAspect setRecordTemplate(@Nonnull RecordTemplate recordTemplate) {
    throw new NotImplementedException();
  }

  @Nonnull
  @Override
  public SystemAspect setAuditStamp(@Nonnull AuditStamp auditStamp) {
    throw new NotImplementedException();
  }

  @Nonnull
  @Override
  public SystemAspect setDatabaseAspect(@Nonnull SystemAspect databaseAspect) {
    throw new NotImplementedException();
  }

  @Nonnull
  @Override
  public Optional<SystemAspect> getDatabaseAspect() {
    return Optional.empty();
  }
}
