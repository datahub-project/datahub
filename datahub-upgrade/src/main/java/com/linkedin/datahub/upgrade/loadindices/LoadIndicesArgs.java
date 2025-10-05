package com.linkedin.datahub.upgrade.loadindices;

import java.util.Collection;
import lombok.Data;

@Data
public class LoadIndicesArgs {
  public int batchSize;
  public int limit;
  public String urnLike;
  public Long lePitEpochMs;
  public Long gePitEpochMs;
  public Collection<String> aspectNames;

  public LoadIndicesArgs clone() {
    LoadIndicesArgs cloned = new LoadIndicesArgs();
    cloned.batchSize = this.batchSize;
    cloned.limit = this.limit;
    cloned.urnLike = this.urnLike;
    cloned.lePitEpochMs = this.lePitEpochMs;
    cloned.gePitEpochMs = this.gePitEpochMs;
    cloned.aspectNames = this.aspectNames;
    return cloned;
  }
}
