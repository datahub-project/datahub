package com.linkedin.metadata.config.postgres;

import lombok.Value;

/** Resolved when {@code postgres.pgSearch.entity.enabled} is true. */
@Value
public class PgSearchEntitySetupOptions {
  String schema;
  String tablePrefix;
  int tierTsvectorColumnCount;
  boolean vectorEnabled;
  int embeddingDimensions;
  String fulltextDefaultLanguage;
}
