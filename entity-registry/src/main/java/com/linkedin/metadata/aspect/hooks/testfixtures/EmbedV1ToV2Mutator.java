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
 * <p>Migrates the {@code embed} aspect from schema version 1 to 2.
 *
 * <p>v1 stored a {@code renderUrl} field (iframe URL). This mutator migrates that data into the v2+
 * structured form: {@code embedType="iframe"} + {@code embedTitle="[v1->v2] <renderUrl>"}. The
 * bracketed marker is read by ZDU smoke-test validators to prove this mutator actually fired on
 * sweep-migrated rows (not just that the schemaVersion was bumped).
 */
public class EmbedV1ToV2Mutator extends AspectMigrationMutator {

  static final String MARKER = "[v1->v2]";

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
    Object renderUrlObj = newData.remove("renderUrl");
    String renderUrl = renderUrlObj != null ? renderUrlObj.toString() : "";
    // Stamp the v2 fields with a verifiable provenance marker — smoke tests
    // grep embedTitle for "[v1->v2]" to assert this mutator fired.
    newData.put("embedType", "iframe");
    newData.put("embedTitle", MARKER + " " + renderUrl);
    return new Embed(newData);
  }
}
