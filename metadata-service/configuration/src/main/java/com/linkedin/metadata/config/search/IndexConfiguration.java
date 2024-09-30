package com.linkedin.metadata.config.search;

import lombok.Data;

@Data
public class IndexConfiguration {
  private String prefix;
  private DocIdsConfiguration docIds;
}
