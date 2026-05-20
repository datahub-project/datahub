package com.linkedin.metadata.aspect.hooks;

import com.linkedin.common.Embed;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.RetrieverContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Migrates the {@code embed} aspect from schema version 1 to 2.
 *
 * <p>v1 stored a {@code renderUrl} field (iframe URL). v2 removes that field; the replacement
 * fields ({@code embedType}, {@code embedTitle}) are introduced in v3.
 */
public class EmbedV1ToV2Mutator extends AspectMigrationMutator {

  @Nonnull
  @Override
  public String getAspectName() {
    return "embed";
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
    DataMap newData = new DataMap(sourceAspect.data());
    newData.remove("renderUrl");
    return new Embed(newData);
  }
}
