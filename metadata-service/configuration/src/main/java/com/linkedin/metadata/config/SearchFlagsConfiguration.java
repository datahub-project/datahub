package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "searchFlags" configuration block in application.yaml.on.yml */
@Data
public class SearchFlagsConfiguration {
  /** Default value for skipHighlighing flag */
  public Boolean defaultSkipHighlighting;
}
