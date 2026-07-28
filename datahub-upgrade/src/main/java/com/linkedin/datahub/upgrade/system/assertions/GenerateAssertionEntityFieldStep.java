package com.linkedin.datahub.upgrade.system.assertions;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.Constants.ASSERTION_ENTITY_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.datahub.upgrade.system.AbstractMCPStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

/**
 * This basically just kicks off AssertionInfoMutator and writes the entity field to the database
 */
@Slf4j
public class GenerateAssertionEntityFieldStep extends AbstractMCPStep {

  public GenerateAssertionEntityFieldStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    super(opContext, entityService, aspectDao, batchSize, batchDelayMs, limit);
  }

  @Override
  public String id() {
    return "assertion-entity-field-v3";
  }

  @Nonnull
  @Override
  protected List<String> getAspectNames() {
    return List.of(ASSERTION_INFO_ASPECT_NAME);
  }

  @VisibleForTesting
  @Nullable
  @Override
  public String getUrnLike() {
    return "urn:li:" + ASSERTION_ENTITY_NAME + ":%";
  }

  /**
   * Continue processing when validation fails.
   *
   * <p>Legacy assertion data may fail validation (e.g., missing entityUrn that AssertionInfoMutator
   * would normally populate). Since this step's purpose is to trigger the mutator to fix such
   * issues, we continue processing even when validation fails. Assertions that cannot be fixed will
   * be logged and can be cleaned up separately.
   */
  @VisibleForTesting
  @Override
  public boolean continueOnValidationFailure() {
    return true;
  }
}
