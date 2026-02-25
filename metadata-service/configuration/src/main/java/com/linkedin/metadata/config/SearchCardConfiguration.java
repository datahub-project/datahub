package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "searchCard" configuration block in application.yaml.on.yml */
@Data
public class SearchCardConfiguration {
  /** If turned on, show the description in search card */
  public Boolean showDescription;
}
