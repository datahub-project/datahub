package com.linkedin.metadata.config.search;

import com.linkedin.metadata.config.shared.LimitConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class SearchServiceConfiguration {
  private QueryFilterRewriterConfiguration queryFilterRewriter;
  private LimitConfig limit;
}
