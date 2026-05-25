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
 * <p>Migrates the {@code embed} aspect from schema version 2 to 3.
 *
 * <p>v2 and v3 share the same field set ({@code embedType}, {@code embedTitle} are optional in
 * both). This mutator appends a provenance marker to {@code embedTitle} so its execution can be
 * verified in tests.
 *
 * <p>NOTE: this bean is intentionally commented out in {@code ZduTestMutatorConfiguration} to
 * simulate a ZDU rolling-upgrade scenario where the v2→v3 hop is missing on old nodes. As a
 * consequence, smoke-test assertions on sweep-migrated rows expect the {@code [v2->v3]} marker to
 * be ABSENT (the chain bridges over the missing hop), proving the bridgeGap path is exercised.
 */
public class EmbedV2ToV3Mutator extends AspectMigrationMutator {

  static final String MARKER = "[v2->v3]";

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
    DataMap newData = new DataMap(sourceAspect.data());
    Object existingTitle = newData.get("embedTitle");
    String newTitle = existingTitle == null ? MARKER : existingTitle.toString() + " " + MARKER;
    newData.put("embedTitle", newTitle);
    return new Embed(newData);
  }
}
