package com.linkedin.metadata.dao;

import io.ebean.Model;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Getter;


@Getter
@Entity
@Table(name = "metadata_id")
public class EbeanIdGenerator extends Model {

  private static final long serialVersionUID = 1L;

  @Id
  long id;
}
