package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "searchBar" configuration block in application.yaml.on.yml */
@Data
public class SearchBarConfiguration {
  /** API variant */
  public String apiVariant;
}
