package com.linkedin.metadata.aspect.hooks.testfixtures;

import com.linkedin.common.GlobalTags;
import com.linkedin.data.DataList;
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
 * <p>Migrates the {@code globalTags} aspect from schema version 1 to 2.
 *
 * <p>v2 adds an optional {@code displayName} field to {@link GlobalTags}. This mutator populates
 * {@code displayName} with a provenance marker derived from the tag count (e.g. {@code "[v1->v2]
 * tagCount=3"}) when the field is unset, so smoke-test validators can verify the migration actually
 * fired. Existing {@code displayName} values are preserved.
 */
public class GlobalTagsV1ToV2Mutator extends AspectMigrationMutator {

  static final String MARKER_PREFIX = "[v1->v2] tagCount=";

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
    DataMap newData = new DataMap(sourceAspect.data());
    if (newData.get("displayName") == null) {
      Object tagsObj = newData.get("tags");
      int tagCount = tagsObj instanceof DataList ? ((DataList) tagsObj).size() : 0;
      newData.put("displayName", MARKER_PREFIX + tagCount);
    }
    return new GlobalTags(newData);
  }
}
