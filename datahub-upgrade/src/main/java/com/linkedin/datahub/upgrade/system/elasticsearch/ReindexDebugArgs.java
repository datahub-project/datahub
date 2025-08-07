package com.linkedin.datahub.upgrade.system.elasticsearch;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class ReindexDebugArgs implements Cloneable {
  public String index;

  @Override
  public ReindexDebugArgs clone() {
    try {
      ReindexDebugArgs clone = (ReindexDebugArgs) super.clone();
      // TODO: copy mutable state here, so the clone can't change the internals of the original
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new AssertionError();
    }
  }
}
