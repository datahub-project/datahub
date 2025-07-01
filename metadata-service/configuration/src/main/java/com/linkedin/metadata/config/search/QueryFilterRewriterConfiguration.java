package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class QueryFilterRewriterConfiguration {

  private ExpansionRewriterConfiguration containerExpansion;
  private ExpansionRewriterConfiguration domainExpansion;

  @NoArgsConstructor
  @AllArgsConstructor
  @Data
  public static class ExpansionRewriterConfiguration {
    public static final ExpansionRewriterConfiguration DEFAULT =
        new ExpansionRewriterConfiguration(false, 100, 100);

    boolean enabled;
    private int pageSize;
    private int limit;
  }
}
