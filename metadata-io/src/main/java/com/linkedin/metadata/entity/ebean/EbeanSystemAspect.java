package com.linkedin.metadata.entity.ebean;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Used to maintain state for the Ebean supported database. This class tracks changes to the
 * entity/aspect and finally produces the ORM object used by {EbeanAspectDao} for version 0.
 *
 * <p>It offers the SystemAspect interface in order to update non-primitives during processing,
 * rather than the serialized values in the base ORM model.
 */
@Accessors(chain = true)
@Builder
@AllArgsConstructor
public class EbeanSystemAspect implements SystemAspect {

  /**
   * When updating an existing row, this stores its state and is returned with updates rather than
   * generating a new orm model for insert
   */
  @Nullable private EbeanAspectV2 ebeanAspectV2;

  @Getter @Nonnull private final Urn urn;

  @Getter @Nonnull private final String aspectName;

  @Getter @Nonnull private final EntitySpec entitySpec;

  @Getter @Nonnull private final AspectSpec aspectSpec;

  @Getter @Setter @Nullable private RecordTemplate recordTemplate;

  @Getter @Setter @Nullable private SystemMetadata systemMetadata;

  @Setter @Nullable private AuditStamp auditStamp;

  /**
   * Return the ebean model for the given version. If version 0, use latest, otherwise copy and
   * update system metadata version.
   *
   * @param version the target version
   * @return the ebean model
   */
  @Override
  @Nonnull
  public EntityAspect withVersion(long version) {
    if (systemMetadata == null) {
      throw new IllegalStateException("Cannot save without system metadata");
    }

    AuditStamp insertAuditStamp = Objects.requireNonNull(auditStamp);
    return new EbeanAspectV2(
            urn.toString(),
            aspectName,
            version,
            RecordUtils.toJsonString(Objects.requireNonNull(recordTemplate)),
            new Timestamp(insertAuditStamp.getTime()),
            insertAuditStamp.getActor().toString(),
            Optional.ofNullable(insertAuditStamp.getImpersonator()).map(Urn::toString).orElse(null),
            RecordUtils.toJsonString(systemMetadata))
        .toEntityAspect();
  }

  @Nonnull
  @Override
  public Optional<SystemAspect> getDatabaseAspect() {
    return Optional.ofNullable(ebeanAspectV2)
        .map(
            ebeanAspect ->
                EntityAspect.EntitySystemAspect.builder()
                    .forUpdate(entitySpec, aspectSpec, ebeanAspect.toEntityAspect()));
  }

  public static class EbeanSystemAspectBuilder {
    private EbeanSystemAspect build() {
      return new EbeanSystemAspect(
          this.ebeanAspectV2,
          this.urn,
          this.aspectName,
          this.entitySpec,
          this.aspectSpec,
          this.recordTemplate,
          this.systemMetadata,
          this.auditStamp);
    }

    public EbeanSystemAspect forUpdate(
        @Nonnull EbeanAspectV2 ebeanAspectV2,
        @Nonnull EntitySpec entitySpec,
        @Nonnull AspectSpec aspectSpec) {
      this.entitySpec = entitySpec;
      this.aspectSpec = aspectSpec;
      this.ebeanAspectV2 = ebeanAspectV2;
      this.urn = UrnUtils.getUrn(ebeanAspectV2.getUrn());
      this.aspectName = ebeanAspectV2.getAspect();
      this.recordTemplate =
          Optional.ofNullable(ebeanAspectV2.getMetadata())
              .map(
                  rawRecordTemplate ->
                      RecordUtils.toRecordTemplate(
                          aspectSpec.getDataTemplateClass(), rawRecordTemplate))
              .orElse(null);
      this.systemMetadata =
          Optional.ofNullable(ebeanAspectV2.getSystemMetadata())
              .map(
                  rawSystemMetadata ->
                      RecordUtils.toRecordTemplate(SystemMetadata.class, rawSystemMetadata))
              .orElse(SystemMetadataUtils.createDefaultSystemMetadata());
      this.auditStamp =
          extractAuditStamp(
              ebeanAspectV2.getCreatedBy(),
              ebeanAspectV2.getCreatedOn().getTime(),
              ebeanAspectV2.getCreatedFor());
      return build();
    }

    public EbeanSystemAspect forUpdate(
        @Nonnull EbeanAspectV2 ebeanAspectV2, @Nonnull EntityRegistry entityRegistry) {
      EntitySpec entitySpec =
          entityRegistry.getEntitySpec(UrnUtils.getUrn(ebeanAspectV2.getUrn()).getEntityType());
      AspectSpec aspectSpec = entitySpec.getAspectSpec(ebeanAspectV2.getAspect());
      return forUpdate(ebeanAspectV2, entitySpec, aspectSpec);
    }

    public EbeanSystemAspect forInsert(
        @Nonnull Urn urn,
        @Nonnull String aspectName,
        @Nonnull EntitySpec entitySpec,
        @Nonnull AspectSpec aspectSpec,
        RecordTemplate recordTemplate,
        SystemMetadata systemMetadata,
        AuditStamp auditStamp) {
      this.urn = urn;
      this.aspectName = aspectName;
      this.entitySpec = entitySpec;
      this.aspectSpec = aspectSpec;
      this.recordTemplate = recordTemplate;
      this.systemMetadata = systemMetadata;
      this.auditStamp = auditStamp;
      return build();
    }
  }

  @Override
  public long getVersion() {
    return ebeanAspectV2 == null ? 0 : ebeanAspectV2.getVersion();
  }

  @Override
  public Timestamp getCreatedOn() {
    return auditStamp == null ? null : new Timestamp(auditStamp.getTime());
  }

  @Override
  public String getCreatedBy() {
    return auditStamp == null ? null : auditStamp.getActor().toString();
  }

  @Nonnull
  @Override
  public SystemAspect setDatabaseAspect(@Nonnull SystemAspect databaseAspect) {
    this.ebeanAspectV2 =
        EbeanAspectV2.fromEntityAspect(databaseAspect.withVersion(databaseAspect.getVersion()));
    return this;
  }

  @Nonnull
  @Override
  public SystemAspect copy() {
    try {
      RecordTemplate recordTemplateCopy = null;
      if (recordTemplate != null) {
        recordTemplateCopy =
            RecordUtils.toRecordTemplate(
                aspectSpec.getDataTemplateClass(), recordTemplate.copy().data());
      } else if (ebeanAspectV2 != null) {
        recordTemplateCopy =
            RecordUtils.toRecordTemplate(
                aspectSpec.getDataTemplateClass(), ebeanAspectV2.getMetadata());
      }

      SystemMetadata systemMetadataCopy = null;
      if (systemMetadata != null) {
        systemMetadataCopy = new SystemMetadata(this.systemMetadata.copy().data());
      } else if (ebeanAspectV2 != null) {
        systemMetadataCopy =
            RecordUtils.toRecordTemplate(SystemMetadata.class, ebeanAspectV2.getSystemMetadata());
      }

      AuditStamp auditStampCopy = null;
      if (auditStamp != null) {
        auditStampCopy = new AuditStamp(this.auditStamp.copy().data());
      } else if (ebeanAspectV2 != null) {
        auditStampCopy =
            extractAuditStamp(
                ebeanAspectV2.getCreatedBy(),
                ebeanAspectV2.getCreatedOn().getTime(),
                ebeanAspectV2.getCreatedFor());
      }

      return new EbeanSystemAspect(
          null, // do not copy the db object
          this.urn,
          this.aspectName,
          this.entitySpec,
          this.aspectSpec,
          recordTemplateCopy,
          systemMetadataCopy,
          auditStampCopy);
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  private static AuditStamp extractAuditStamp(
      @Nonnull String createdBy, long createdOn, @Nullable String createdFor) {
    return new AuditStamp()
        .setActor(UrnUtils.getUrn(createdBy))
        .setTime(createdOn)
        .setImpersonator(
            Optional.ofNullable(createdFor).map(UrnUtils::getUrn).orElse(null),
            SetMode.IGNORE_NULL);
  }
}
