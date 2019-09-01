package com.linkedin.datahub.models.view;

import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class DatasetView {

  private String platform;

  private String nativeName;

  private String fabric;

  private String uri;

  private String description;

  private String nativeType;

  private String properties;

  private List<String> tags;

  private Boolean removed;

  private Boolean deprecated;

  private String deprecationNote;

  private Long decommissionTime;

  private Long createdTime;

  private Long modifiedTime;
}
