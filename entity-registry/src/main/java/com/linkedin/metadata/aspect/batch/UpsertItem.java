package com.linkedin.metadata.aspect.batch;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A proposal to write data to the primary datastore which includes system metadata and other
 * related data stored along with the aspect
 */
public abstract class UpsertItem extends MCPBatchItem {
  public abstract RecordTemplate getAspect();

  public abstract SystemAspect toLatestEntityAspect();

  public abstract void validatePreCommit(
      @Nullable RecordTemplate previous, @Nonnull AspectRetriever aspectRetriever)
      throws AspectValidationException;
}
