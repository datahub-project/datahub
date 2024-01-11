package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.batch.SystemAspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * This is an internal representation of an entity aspect record {@link EntityServiceImpl} and
 * {@link AspectDao} implementations are using. While {@link AspectDao} implementations have their
 * own aspect record implementations, they cary implementation details that should not leak outside.
 * Therefore, this is the type to use in public {@link AspectDao} methods.
 */
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

  public EntityAspectIdentifier toAspectIdentifier() {
    return new EntityAspectIdentifier(getUrn(), getAspect(), getVersion());
  }

  @Nonnull
  public SystemAspect asSystemAspect() {
    return EntitySystemAspect.from(this);
  }

  /**
   * Provide a typed EntityAspect without breaking the existing public contract with generic types.
   */
  @Getter
  @AllArgsConstructor
  @EqualsAndHashCode
  public static class EntitySystemAspect implements SystemAspect {

    @Nullable
    public static EntitySystemAspect from(EntityAspect entityAspect) {
      return entityAspect != null ? new EntitySystemAspect(entityAspect) : null;
    }

    @Nonnull private final EntityAspect entityAspect;

    @Nonnull
    public Urn getUrn() {
      try {
        return Urn.createFromString(entityAspect.getUrn());
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Nonnull
    public String getUrnRaw() {
      return entityAspect.getUrn();
    }

    @Override
    public SystemMetadata getSystemMetadata() {
      return EntityUtils.parseSystemMetadata(entityAspect.getSystemMetadata());
    }

    @Nullable
    public String getSystemMetadataRaw() {
      return entityAspect.getSystemMetadata();
    }

    @Override
    public Timestamp getCreatedOn() {
      return entityAspect.getCreatedOn();
    }

    @Override
    public String getAspectName() {
      return entityAspect.aspect;
    }

    @Override
    public long getVersion() {
      return entityAspect.getVersion();
    }

    @Override
    public RecordTemplate getRecordTemplate(EntityRegistry entityRegistry) {
      return EntityUtils.toAspectRecord(
          getUrn().getEntityType(), getAspectName(), entityAspect.getMetadata(), entityRegistry);
    }

    public EntityAspect asRaw() {
      return entityAspect;
    }
  }
}
