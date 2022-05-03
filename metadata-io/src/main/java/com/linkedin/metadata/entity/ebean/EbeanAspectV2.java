package com.linkedin.metadata.entity.ebean;

import com.linkedin.metadata.entity.EntityAspect;
import io.ebean.Model;
import io.ebean.annotation.Index;
import java.sql.Timestamp;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;


/**
 * Schema definition for the new aspect table.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "metadata_aspect_v2")
public class EbeanAspectV2 extends Model implements EntityAspect {

  private static final long serialVersionUID = 1L;

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

  /**
   * Key for an aspect in the table.
   */
  @Embeddable
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @EqualsAndHashCode
  public static class PrimaryKey {

    private static final long serialVersionUID = 1L;

    @NonNull
    @Index
    @Column(name = URN_COLUMN, length = 500, nullable = false)
    private String urn;

    @NonNull
    @Index
    @Column(name = ASPECT_COLUMN, length = 200, nullable = false)
    private String aspect;

    @Index
    @Column(name = VERSION_COLUMN, nullable = false)
    private long version;
  }

  @NonNull
  @EmbeddedId
  @Index
  protected PrimaryKey key;

  @NonNull
  @Column(name = URN_COLUMN, length = 500, nullable = false)
  private String urn;

  @NonNull
  @Column(name = ASPECT_COLUMN, length = 200, nullable = false)
  private String aspect;

  @Column(name = VERSION_COLUMN, nullable = false)
  private long version;

  @NonNull
  @Lob
  @Column(name = METADATA_COLUMN, nullable = false)
  protected String metadata;

  @NonNull
  @Column(name = CREATED_ON_COLUMN, nullable = false)
  private Timestamp createdOn;

  @NonNull
  @Column(name = CREATED_BY_COLUMN, nullable = false)
  private String createdBy;

  @Column(name = CREATED_FOR_COLUMN, nullable = true)
  private String createdFor;

  @Column(name = SYSTEM_METADATA_COLUMN, nullable = true)
  protected String systemMetadata;

  public EbeanAspectV2(String urn, String aspect, long version, String metadata, Timestamp createdOn, String createdBy,
      String createdFor, String systemMetadata) {
    this(new PrimaryKey(urn, aspect, version), urn, aspect, version, metadata, createdOn, createdBy, createdFor,
        systemMetadata);
  }
}
