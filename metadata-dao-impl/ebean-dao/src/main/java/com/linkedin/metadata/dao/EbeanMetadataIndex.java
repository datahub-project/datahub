package com.linkedin.metadata.dao;

import io.ebean.annotation.Index;
import io.ebean.Model;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;


@Getter
@Setter
// define composite indexes
@Index(name = "idx_long_val", columnNames = {
    EbeanMetadataIndex.ASPECT_COLUMN,
    EbeanMetadataIndex.PATH_COLUMN,
    EbeanMetadataIndex.LONG_COLUMN,
    EbeanMetadataIndex.URN_COLUMN
})
@Index(name = "idx_string_val", columnNames = {
    EbeanMetadataIndex.ASPECT_COLUMN,
    EbeanMetadataIndex.PATH_COLUMN,
    EbeanMetadataIndex.STRING_COLUMN,
    EbeanMetadataIndex.URN_COLUMN
})
@Index(name = "idx_double_val", columnNames = {
    EbeanMetadataIndex.ASPECT_COLUMN,
    EbeanMetadataIndex.PATH_COLUMN,
    EbeanMetadataIndex.DOUBLE_COLUMN,
    EbeanMetadataIndex.URN_COLUMN
})
@Entity
@Accessors(chain = true)
@Table(name = "metadata_index")
public class EbeanMetadataIndex extends Model {

  public static final long serialVersionUID = 1L;

  private static final String ID_COLUMN = "id";
  public static final String URN_COLUMN = "urn";
  public static final String ASPECT_COLUMN = "aspect";
  public static final String PATH_COLUMN = "path";
  public static final String LONG_COLUMN = "longVal";
  public static final String STRING_COLUMN = "stringVal";
  public static final String DOUBLE_COLUMN = "doubleVal";

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = ID_COLUMN)
  protected long id;

  @NonNull
  @Index(name = "idx_urn")
  @Column(name = URN_COLUMN, length = 500, nullable = false)
  protected String urn;

  @NonNull
  @Column(name = ASPECT_COLUMN, length = 200, nullable = false)
  protected String aspect;

  @NonNull
  @Column(name = PATH_COLUMN, length = 200, nullable = false)
  protected String path;

  @Column(name = LONG_COLUMN)
  protected Long longVal;

  @Column(name = STRING_COLUMN, length = 500)
  protected String stringVal;

  @Column(name = DOUBLE_COLUMN)
  protected Double doubleVal;
}