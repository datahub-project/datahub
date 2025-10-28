package com.linkedin.metadata.config.search;

import lombok.Data;

@Data
public class IndexConfiguration {
  private String prefix;
  private DocIdsConfiguration docIds;
  private Integer minSearchFilterLength;

  public String getFinalPrefix() {
    if (prefix == null || prefix.isEmpty()) {
      return "";
    } else {
      return prefix + "_";
    }
  }
}
