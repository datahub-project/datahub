package com.linkedin.metadata.aspect.batch;

import com.linkedin.data.template.RecordTemplate;

/**
 * A proposal to write data to the primary datastore which includes system metadata and other
 * related data stored along with the aspect
 *
 * @param <S>
 */
public abstract class UpsertItem<S extends SystemAspect> extends MCPBatchItem {
  public abstract RecordTemplate getAspect();

  public abstract S toLatestEntityAspect();
}
