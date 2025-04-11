package com.linkedin.test.metadata.aspect.batch;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class TestSystemAspect implements SystemAspect {
  private Urn urn;
  private long version;
  @Setter private RecordTemplate recordTemplate;
  @Setter private SystemMetadata systemMetadata;
  @Setter private AuditStamp auditStamp;
  private EntitySpec entitySpec;
  private AspectSpec aspectSpec;

  @Override
  public Timestamp getCreatedOn() {
    return auditStamp != null ? new Timestamp(auditStamp.getTime()) : null;
  }

  @Override
  public String getCreatedBy() {
    return auditStamp != null ? auditStamp.getActor().toString() : null;
  }

  @Override
  @Nonnull
  public EntityAspect withVersion(long version) {
    return EntityAspect.builder()
        .urn(urn.toString())
        .aspect(aspectSpec.getName())
        .version(version)
        .metadata(Optional.ofNullable(recordTemplate).map(RecordUtils::toJsonString).orElse(null))
        .systemMetadata(
            Optional.ofNullable(systemMetadata).map(RecordUtils::toJsonString).orElse(null))
        .createdBy(
            Optional.ofNullable(auditStamp)
                .map(AuditStamp::getActor)
                .map(Urn::toString)
                .orElse(null))
        .createdOn(
            Optional.ofNullable(auditStamp).map(a -> new Timestamp(a.getTime())).orElse(null))
        .createdFor(
            Optional.ofNullable(auditStamp)
                .map(AuditStamp::getImpersonator)
                .map(Urn::toString)
                .orElse(null))
        .build();
  }

  @Nonnull
  @Override
  public SystemAspect copy() {
    return this.toBuilder().build();
  }

  @Nonnull
  @Override
  public Optional<SystemAspect> getDatabaseAspect() {
    return Optional.empty();
  }

  @Nonnull
  @Override
  public SystemAspect setDatabaseAspect(@Nonnull SystemAspect databaseAspect) {
    return this;
  }
}
