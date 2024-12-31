package com.linkedin.metadata.entity.ebean;

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

/** Schema definition for the legacy aspect table. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "metadata_aspect")
public class EbeanAspectV1 extends Model {

  public static final String ALL_COLUMNS = "*";
  public static final String KEY_ID = "key";
  public static final String URN_COLUMN = "urn";
  public static final String ASPECT_COLUMN = "aspect";
  public static final String VERSION_COLUMN = "version";
  public static final String METADATA_COLUMN = "metadata";
  public static final String CREATED_ON_COLUMN = "createdOn";
  public static final String CREATED_BY_COLUMN = "createdBy";
  public static final String CREATED_FOR_COLUMN = "createdFor";

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
  }

  @Nonnull @EmbeddedId @Index protected PrimaryKey key;

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
}
