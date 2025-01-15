package com.linkedin.metadata.entity;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
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
  @Builder
  @Getter
  @EqualsAndHashCode
  public static class EntitySystemAspect implements SystemAspect {
    @Nonnull private final EntityAspect entityAspect;
    @Nonnull private final Urn urn;

    /** Note that read mutations depend on the mutability of recordTemplate */
    @Nullable private final RecordTemplate recordTemplate;

    @Nonnull private final EntitySpec entitySpec;
    @Nullable private final AspectSpec aspectSpec;

    @Nonnull
    public String getUrnRaw() {
      return entityAspect.getUrn();
    }

    @Nullable
    public String getSystemMetadataRaw() {
      return entityAspect.getSystemMetadata();
    }

    public String getMetadataRaw() {
      return entityAspect.getMetadata();
    }

    @Override
    public Timestamp getCreatedOn() {
      return entityAspect.getCreatedOn();
    }

    @Override
    public String getCreatedBy() {
      return entityAspect.getCreatedBy();
    }

    @Override
    @Nonnull
    public String getAspectName() {
      return entityAspect.aspect;
    }

    @Override
    public long getVersion() {
      return entityAspect.getVersion();
    }

    @Nullable
    public SystemMetadata getSystemMetadata() {
      return EntityApiUtils.parseSystemMetadata(getSystemMetadataRaw());
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

    @Override
    public String toString() {
      return entityAspect.toString();
    }

    public static class EntitySystemAspectBuilder {

      private EntityAspect.EntitySystemAspect build() {
        return null;
      }

      public EntityAspect.EntitySystemAspect build(
          @Nonnull EntitySpec entitySpec,
          @Nullable AspectSpec aspectSpec,
          @Nonnull EntityAspect entityAspect) {
        this.entityAspect = entityAspect;
        this.urn = UrnUtils.getUrn(entityAspect.getUrn());
        this.aspectSpec = aspectSpec;
        if (entityAspect.getMetadata() != null) {
          this.recordTemplate =
              RecordUtils.toRecordTemplate(
                  (Class<? extends RecordTemplate>)
                      (aspectSpec == null
                          ? GenericAspect.class
                          : aspectSpec.getDataTemplateClass()),
                  entityAspect.getMetadata());
        }

        return new EntitySystemAspect(entityAspect, urn, recordTemplate, entitySpec, aspectSpec);
      }
    }
  }
}
