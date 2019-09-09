package com.linkedin.datahub.models.view;

import java.io.Serializable;
import lombok.Data;


@Data
public class DatasetOwner {

  private String userName;

  private String source;

  private String namespace;

  private String name;

  private String email;

  private Boolean isGroup;

  private Boolean isActive;

  private String idType;

  private String type;

  private String subType;

  private Integer sortId;

  private String sourceUrl;

  private String confirmedBy;

  private Long modifiedTime;

  private String pictureLink;

  static class DatasetOwnerKey implements Serializable {
    private String userName;
    private String source;
  }
}
