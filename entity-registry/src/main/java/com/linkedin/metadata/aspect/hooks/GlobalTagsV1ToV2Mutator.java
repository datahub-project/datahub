package com.linkedin.metadata.aspect.hooks;

import com.linkedin.common.GlobalTags;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.RetrieverContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Migrates the {@code globalTags} aspect from schema version 1 to 2.
 *
 * <p>v2 adds an optional {@code displayName} field to {@link GlobalTags}. Since the field is
 * optional and absent-is-valid for existing records, this mutator advances the schema version with
 * no data transformation.
 */
public class GlobalTagsV1ToV2Mutator extends AspectMigrationMutator {

  @Nonnull
  @Override
  public String getAspectName() {
    return "globalTags";
  }

  @Override
  public long getSourceVersion() {
    return 1;
  }

  @Override
  public long getTargetVersion() {
    return 2;
  }

  @Nullable
  @Override
  protected RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
    return new GlobalTags(new DataMap(sourceAspect.data()));
  }
}
