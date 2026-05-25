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
 * <p>v3 and v4 are structurally identical; this mutator appends a provenance marker to {@code
 * embedTitle} so its execution can be verified in tests.
 */
public class EmbedV3ToV4Mutator extends AspectMigrationMutator {

  static final String MARKER = "[v3->v4]";

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
    DataMap newData = new DataMap(sourceAspect.data());
    Object existingTitle = newData.get("embedTitle");
    String newTitle = existingTitle == null ? MARKER : existingTitle.toString() + " " + MARKER;
    newData.put("embedTitle", newTitle);
    return new Embed(newData);
  }
}
