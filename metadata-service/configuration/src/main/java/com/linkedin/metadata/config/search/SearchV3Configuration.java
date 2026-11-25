package com.linkedin.metadata.config.search;

import lombok.Data;
import lombok.experimental.Accessors;

/** Search V3 features that live under {@code elasticsearch.search.v3}. */
@Data
@Accessors(chain = true)
public class SearchV3Configuration {
  /**
   * Copy a referenced business attribute's searchable fields into the V3 document. Off stores the
   * attribute URN from the event and does not read the attribute.
   */
  private boolean businessAttributeExpansionEnabled = false;

  /** Project semanticContent embeddings into the V3 document. Off leaves embeddings unset. */
  private boolean vectorProjectionEnabled = false;

  private SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
}
