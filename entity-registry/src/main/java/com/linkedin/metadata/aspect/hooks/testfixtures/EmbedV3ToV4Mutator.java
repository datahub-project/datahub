package com.linkedin.metadata.aspect.hooks.testfixtures;

import com.linkedin.common.Embed;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Test-only fixture — not a production aspect migration. Exists solely to exercise the {@link
 * AspectMigrationMutator} infrastructure end-to-end via the ZDU smoke-test framework. Production
 * deployments keep the bean disabled (the {@code featureFlags.aspectMigrationMutatorEnabled}
 * property is {@code false} by default).
 *
 * <p>Migrates the {@code embed} aspect from schema version 3 to 4.
 *
 * <p>v3 and v4 are structurally identical; this mutator advances the stored schema version with no
 * data transformation, bringing the version in line with the current PDL declaration.
 */
public class EmbedV3ToV4Mutator extends AspectMigrationMutator {

  @Nonnull
  @Override
  public String getAspectName() {
    return "embed";
  }

  @Override
  public long getSourceVersion() {
    return 3;
  }

  @Override
  public long getTargetVersion() {
    return 4;
  }

  @Nullable
  @Override
  protected RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
    return new Embed(new DataMap(sourceAspect.data()));
  }
}
