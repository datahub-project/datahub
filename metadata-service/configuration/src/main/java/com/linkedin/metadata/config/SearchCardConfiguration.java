package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "searchCard" configuration block in application.yaml.on.yml */
@Data
public class SearchCardConfiguration {
  /** If turned on, show the description in search card */
  public Boolean showDescription;

  /** Structured property URN used as the AI summary source on search cards */
  public String summarySourceUrn;

  /** Label displayed on the summary pill in search cards. Defaults to "AI Summary". */
  public String summaryLabel;
}
