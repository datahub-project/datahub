package com.linkedin.metadata.aspect.hooks.migrations;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * v1 → v2 migration for {@code dataProductProperties}.
 *
 * <p>No payload reshape is required: bumping the schema version forces MigrateAspects / ZDU to
 * rewrite existing Data Product properties. That rewrite is payload-identical, so GMS treats it as
 * a no-op and does not emit the MCL {@code DataProductAssetsSideEffect} needs. Asset-side {@code
 * dataProducts} population is {@code ResyncDataProductAssetsStep}, not this mutator.
 */
@Slf4j
@Component
public class DataProductPropertiesMigrationMutator extends AspectMigrationMutator {

  @Nonnull
  @Override
  public String getAspectName() {
    return DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
  }

  @Override
  public long getSourceVersion() {
    return 1L;
  }

  @Override
  public long getTargetVersion() {
    return 2L;
  }

  @Nullable
  @Override
  protected RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
    DataProductProperties properties = new DataProductProperties(sourceAspect.data());
    try {
      // Always return a copy so schemaVersion is bumped even when the payload is unchanged.
      return properties.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(
          "Failed to copy DataProductProperties aspect for migration", e);
    }
  }
}
