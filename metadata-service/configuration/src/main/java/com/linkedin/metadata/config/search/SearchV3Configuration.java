package com.linkedin.metadata.config.search;

import lombok.Data;
import lombok.experimental.Accessors;

/** Search V3 features that live under {@code elasticsearch.search.v3}. */
@Data
@Accessors(chain = true)
public class SearchV3Configuration {
  private SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
}
