package com.linkedin.metadata.aspect;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * This is an internal representation of an entity aspect record {@link EntityServiceImpl} and
 * {@link AspectDao} implementations are using. While {@link AspectDao} implementations have their
 * own aspect record implementations, they cary implementation details that should not leak outside.
 * Therefore, this is the type to use in public {@link AspectDao} methods.
 */
@Slf4j
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Builder(toBuilder = true)
public class EntityAspect {

  @Nonnull private String urn;

  @Nonnull private String aspect;

  private long version;

  private String metadata;

  private String systemMetadata;

  private Timestamp createdOn;

  private String createdBy;

  private String createdFor;

  @Override
  public String toString() {
    return "EntityAspect{"
        + "urn='"
        + urn
        + '\''
        + ", aspect='"
        + aspect
        + '\''
        + ", version="
        + version
        + ", metadata='"
        + metadata
        + '\''
        + ", systemMetadata='"
        + systemMetadata
        + '\''
        + '}';
  }

  /**
   * Provide a typed EntityAspect without breaking the existing public contract with generic types.
   */
  @Accessors(chain = true)
  @Builder(toBuilder = true)
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor
  public static class EntitySystemAspect implements SystemAspect {
    @Nullable private EntityAspect entityAspect;
    @Nonnull private final Urn urn;

    /** Note that read mutations depend on the mutability of recordTemplate */
    @Setter @Nullable private RecordTemplate recordTemplate;

    @Setter @Nullable private SystemMetadata systemMetadata;
    @Setter @Nullable private AuditStamp auditStamp;

    @Nonnull private final EntitySpec entitySpec;
    @Nullable private final AspectSpec aspectSpec;

    @Nonnull
    public String getUrnRaw() {
      return urn.toString();
    }

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
    public String getAspectName() {
      return aspectSpec.getName();
    }

    @Override
    public long getVersion() {
      return entityAspect == null ? 0 : entityAspect.getVersion();
    }

    /**
     * Convert to enveloped aspect
     *
     * @return enveloped aspect
     */
    public EnvelopedAspect toEnvelopedAspects() {
      // Now turn it into an EnvelopedAspect
      final com.linkedin.entity.Aspect aspect =
          new com.linkedin.entity.Aspect(getRecordTemplate().data());

      final EnvelopedAspect envelopedAspect = new EnvelopedAspect();
      envelopedAspect.setName(getAspectName());
      envelopedAspect.setVersion(getVersion());

      // TODO: I think we can assume this here, adding as it's a required field so object mapping
      // barfs when trying to access it,
      //    since nowhere else is using it should be safe for now at least
      envelopedAspect.setType(AspectType.VERSIONED);
      envelopedAspect.setValue(aspect);

      try {
        if (getSystemMetadata() != null) {
          envelopedAspect.setSystemMetadata(getSystemMetadata());
        }
      } catch (Exception e) {
        log.warn(
            "Exception encountered when setting system metadata on enveloped aspect {}. Error: {}",
            envelopedAspect.getName(),
            e.toString());
      }

      envelopedAspect.setCreated(getAuditStamp());

      return envelopedAspect;
    }

    public static class EntitySystemAspectBuilder {

      private EntityAspect.EntitySystemAspect build() {
        return new EntitySystemAspect(
            this.entityAspect,
            this.urn,
            this.recordTemplate,
            this.systemMetadata,
            this.auditStamp,
            this.entitySpec,
            this.aspectSpec);
      }

      public EntityAspect.EntitySystemAspect forInsert(
          @Nonnull EntityAspect entityAspect, @Nonnull EntityRegistry entityRegistry) {
        this.urn = UrnUtils.getUrn(entityAspect.getUrn());
        this.entitySpec = entityRegistry.getEntitySpec(this.urn.getEntityType());
        this.aspectSpec = entitySpec.getAspectSpec(entityAspect.getAspect());
        fromEntityAspect(entityAspect);
        return build();
      }

      public EntityAspect.EntitySystemAspect forUpdate(
          @Nonnull EntityAspect entityAspect, @Nonnull EntityRegistry entityRegistry) {

        this.entityAspect = entityAspect;

        this.urn = UrnUtils.getUrn(entityAspect.getUrn());
        this.entitySpec = entityRegistry.getEntitySpec(this.urn.getEntityType());
        this.aspectSpec = entitySpec.getAspectSpec(entityAspect.getAspect());
        fromEntityAspect(this.entityAspect);
        return build();
      }

      public EntityAspect.EntitySystemAspect forUpdate(
          @Nonnull EntitySpec entitySpec,
          @Nullable AspectSpec aspectSpec,
          @Nonnull EntityAspect entityAspect) {
        this.entityAspect = entityAspect;
        this.entitySpec = entitySpec;
        this.aspectSpec = aspectSpec;
        fromEntityAspect(this.entityAspect);
        return build();
      }

      private void fromEntityAspect(@Nonnull EntityAspect entityAspect) {
        this.urn = UrnUtils.getUrn(entityAspect.getUrn());
        if (entityAspect.getMetadata() != null) {
          this.recordTemplate =
              RecordUtils.toRecordTemplate(
                  (Class<? extends RecordTemplate>)
                      (aspectSpec == null
                          ? GenericAspect.class
                          : aspectSpec.getDataTemplateClass()),
                  entityAspect.getMetadata());
        }
        if (entityAspect.getSystemMetadata() != null) {
          this.systemMetadata =
              SystemMetadataUtils.parseSystemMetadata(entityAspect.getSystemMetadata());
        }
        if (entityAspect.getCreatedBy() != null) {
          this.auditStamp =
              new AuditStamp()
                  .setActor(UrnUtils.getUrn(entityAspect.getCreatedBy()))
                  .setTime(entityAspect.getCreatedOn().getTime())
                  .setImpersonator(
                      Optional.ofNullable(entityAspect.getCreatedFor())
                          .map(UrnUtils::getUrn)
                          .orElse(null),
                      SetMode.IGNORE_NULL);
        }
      }
    }

    @Nonnull
    @Override
    public SystemAspect copy() {
      return this.toBuilder().entityAspect(null).build();
    }

    @Nonnull
    @Override
    public Optional<SystemAspect> getDatabaseAspect() {
      return Optional.ofNullable(entityAspect)
          .map(a -> EntitySystemAspect.builder().forUpdate(entitySpec, aspectSpec, a));
    }

    @Nonnull
    @Override
    public SystemAspect setDatabaseAspect(@Nonnull SystemAspect databaseAspect) {
      this.entityAspect = databaseAspect.withVersion(databaseAspect.getVersion());
      return this;
    }

    @Override
    @Nonnull
    public EntityAspect withVersion(long version) {
      return new EntityAspect(
          urn.toString(),
          aspectSpec.getName(),
          version,
          Optional.ofNullable(recordTemplate).map(RecordUtils::toJsonString).orElse(null),
          Optional.ofNullable(systemMetadata).map(RecordUtils::toJsonString).orElse(null),
          Optional.ofNullable(auditStamp).map(a -> new Timestamp(a.getTime())).orElse(null),
          Optional.ofNullable(auditStamp).map(AuditStamp::getActor).map(Urn::toString).orElse(null),
          Optional.ofNullable(auditStamp)
              .map(AuditStamp::getImpersonator)
              .map(Urn::toString)
              .orElse(null));
    }
  }
}
