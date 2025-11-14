package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Configuration for embedding update operations. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmbeddingsUpdateConfiguration {
  /** Number of documents to process in each batch */
  private int batchSize = 100;

  /** Maximum number of retry attempts for failed embedding generation */
  private int maxRetries = 3;

  /** Maximum text length (in characters) for embedding generation */
  private int maxTextLength = 8000;
}
