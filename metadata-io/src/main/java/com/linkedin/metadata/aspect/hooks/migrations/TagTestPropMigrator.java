package com.linkedin.metadata.aspect.hooks.migrations;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import com.linkedin.tag.TagProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Migrates {@code tagProperties} from schema version 1 to 2 by populating {@code testProperty} with
 * the existing {@code description} value plus a fixed suffix.
 */
@Slf4j
public class TagTestPropMigrator extends AspectMigrationMutator {

  private static final String ASPECT_NAME = "tagProperties";
  private static final long SOURCE_VERSION = 1L;
  private static final long TARGET_VERSION = 2L;
  static final String SUFFIX = "_migrated";

  @Nonnull
  @Override
  public String getAspectName() {
    return ASPECT_NAME;
  }

  @Override
  public long getSourceVersion() {
    return SOURCE_VERSION;
  }

  @Override
  public long getTargetVersion() {
    return TARGET_VERSION;
  }

  @Nullable
  @Override
  protected RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
    TagProperties props = new TagProperties(sourceAspect.data());
    if (!props.hasDescription()) {
      log.info("no description, skipping");
      return null;
    }
    String description = props.getDescription();
    log.info(
        "Migrating tagProperties v{} -> v{}: setting testProperty from description \"{}\"",
        SOURCE_VERSION,
        TARGET_VERSION,
        description);
    props.setTestProperty(description + SUFFIX);
    return props;
  }
}
