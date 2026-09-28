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
}
