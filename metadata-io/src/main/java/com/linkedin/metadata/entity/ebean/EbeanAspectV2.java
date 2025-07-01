package com.linkedin.metadata.entity.ebean;

import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import io.ebean.Model;
import io.ebean.annotation.Index;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.sql.Timestamp;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Schema definition for the new aspect table. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "metadata_aspect_v2")
public class EbeanAspectV2 extends Model {

  public static final String ALL_COLUMNS = "*";
  public static final String KEY_ID = "key";
  public static final String URN_COLUMN = "urn";
  public static final String ASPECT_COLUMN = "aspect";
  public static final String VERSION_COLUMN = "version";
  public static final String METADATA_COLUMN = "metadata";
  public static final String CREATED_ON_COLUMN = "createdOn";
  public static final String CREATED_BY_COLUMN = "createdBy";
  public static final String CREATED_FOR_COLUMN = "createdFor";

  public static final String SYSTEM_METADATA_COLUMN = "systemmetadata";

  /** Key for an aspect in the table. */
  @Embeddable
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @EqualsAndHashCode
  public static class PrimaryKey implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nonnull
    @Index
    @Column(name = URN_COLUMN, length = 500, nullable = false)
    private String urn;

    @Nonnull
    @Index
    @Column(name = ASPECT_COLUMN, length = 200, nullable = false)
    private String aspect;

    @Index
    @Column(name = VERSION_COLUMN, nullable = false)
    private long version;

    public static PrimaryKey fromAspectIdentifier(EntityAspectIdentifier key) {
      return new PrimaryKey(key.getUrn(), key.getAspect(), key.getVersion());
    }

    public EntityAspectIdentifier toAspectIdentifier() {
      return new EntityAspectIdentifier(getUrn(), getAspect(), getVersion());
    }
  }

  @Nonnull @EmbeddedId @Index protected PrimaryKey key;

  @Nonnull
  @Column(name = URN_COLUMN, length = 500, nullable = false)
  private String urn;

  @Nonnull
  @Column(name = ASPECT_COLUMN, length = 200, nullable = false)
  private String aspect;

  @Column(name = VERSION_COLUMN, nullable = false)
  private long version;

  @Nonnull
  @Lob
  @Column(name = METADATA_COLUMN, nullable = false)
  protected String metadata;

  @Nonnull
  @Column(name = CREATED_ON_COLUMN, nullable = false)
  private Timestamp createdOn;

  @Nonnull
  @Column(name = CREATED_BY_COLUMN, nullable = false)
  private String createdBy;

  @Column(name = CREATED_FOR_COLUMN, nullable = true)
  private String createdFor;

  @Column(name = SYSTEM_METADATA_COLUMN, nullable = true)
  protected String systemMetadata;

  public EbeanAspectV2(
      String urn,
      String aspect,
      long version,
      String metadata,
      Timestamp createdOn,
      String createdBy,
      String createdFor,
      String systemMetadata) {
    this(
        new PrimaryKey(urn, aspect, version),
        urn,
        aspect,
        version,
        metadata,
        createdOn,
        createdBy,
        createdFor,
        systemMetadata);
  }

  @Nonnull
  public EntityAspect toEntityAspect() {
    return new EntityAspect(
        getKey().getUrn(),
        getKey().getAspect(),
        getKey().getVersion(),
        getMetadata(),
        getSystemMetadata(),
        getCreatedOn(),
        getCreatedBy(),
        getCreatedFor());
  }

  public static EbeanAspectV2 fromEntityAspect(EntityAspect aspect) {
    return new EbeanAspectV2(
        aspect.getUrn(),
        aspect.getAspect(),
        aspect.getVersion(),
        aspect.getMetadata(),
        aspect.getCreatedOn(),
        aspect.getCreatedBy(),
        aspect.getCreatedFor(),
        aspect.getSystemMetadata());
  }
}
