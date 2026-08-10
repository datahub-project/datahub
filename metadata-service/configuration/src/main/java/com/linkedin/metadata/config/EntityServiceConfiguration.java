package com.linkedin.metadata.config;

import javax.annotation.Nullable;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class EntityServiceConfiguration {
  private boolean alwaysEmitChangeLog = false;
  private boolean cdcModeChangeLog = false;
  @Nullable private Integer retry = null;
  private boolean enableBrowseV2 = false;
  private boolean postCommitRetentionEnabled = false;

  /**
   * Max items per DB transaction when ingesting aspects. Values &lt;= 0 disable chunking (one txn
   * for the whole request). Default should match {@link
   * EbeanConfiguration#DEFAULT_QUERY_KEYS_COUNT} / {@code ebean.queryKeysCountForBatch}. Oversized
   * API batches are split into successive transactions of this size.
   */
  private int maxRequestBatchSize = 0;
}
