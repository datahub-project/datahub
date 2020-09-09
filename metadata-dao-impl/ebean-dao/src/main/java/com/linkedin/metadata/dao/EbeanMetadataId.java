package com.linkedin.metadata.dao;

import io.ebean.Model;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "metadata_id", uniqueConstraints = {
    @UniqueConstraint(columnNames = {EbeanMetadataId.NAMESPACE_COLUMN, EbeanMetadataId.ID_COLUMN})})
public class EbeanMetadataId extends Model {

  public static final String NAMESPACE_COLUMN = "namespace";
  public static final String ID_COLUMN = "id";

  private static final long serialVersionUID = 1L;

  @NonNull
  @Column(name = NAMESPACE_COLUMN, nullable = false)
  String namespace;

  @Column(name = ID_COLUMN)
  long id;
}
