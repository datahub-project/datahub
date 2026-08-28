package com.linkedin.datahub.upgrade.system.dataproducts;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Streams {@code dataProductProperties} aspects and re-emits MCLs so Elasticsearch documents
 * include {@code hasParentDataProduct} (required by {@code getRootDataProducts}).
 */
public class BackfillDataProductIndicesStep extends AbstractMCLStep {

  private static final String UPGRADE_ID = BackfillDataProductIndices.class.getSimpleName() + "-v1";

  public BackfillDataProductIndicesStep(
      @Nonnull OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    super(opContext, entityService, aspectDao, batchSize, batchDelayMs, limit);
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Nonnull
  @Override
  protected String getAspectName() {
    return DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
  }

  @Nullable
  @Override
  protected String getUrnLike() {
    return "urn:li:" + DATA_PRODUCT_ENTITY_NAME + ":%";
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return true;
  }
}
