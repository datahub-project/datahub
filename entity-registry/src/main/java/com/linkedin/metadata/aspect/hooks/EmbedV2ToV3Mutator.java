package com.linkedin.metadata.aspect.hooks;

import com.linkedin.common.Embed;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.RetrieverContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Migrates the {@code embed} aspect from schema version 2 to 3.
 *
 * <p>v2 and v3 share the same field set ({@code embedType}, {@code embedTitle} are optional in
 * both). This mutator advances the stored schema version with no data transformation.
 *
 * <p>NOTE: this bean is intentionally commented out in {@code SpringStandardPluginConfiguration} to
 * simulate a ZDU rolling-upgrade scenario where the v2→v3 hop is missing on old nodes.
 */
public class EmbedV2ToV3Mutator extends AspectMigrationMutator {

  @Nonnull
  @Override
  public String getAspectName() {
    return "embed";
  }

  @Override
  public long getSourceVersion() {
    return 2;
  }

  @Override
  public long getTargetVersion() {
    return 3;
  }

  @Nullable
  @Override
  protected RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
    return new Embed(new DataMap(sourceAspect.data()));
  }
}
