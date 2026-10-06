package com.linkedin.metadata.aspect.hooks.migrations.criterion;

import static com.linkedin.metadata.Constants.DATAHUB_VIEW_INFO_ASPECT_NAME;

import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

/**
 * v1 → v2: strips legacy {@code value} from {@code definition.filter} criteria (see
 * com.linkedin.metadata.query.filter.Criterion).
 */
@Component
public class ViewInfoCriterionMutator extends CriterionFilterMutatorBase {

  @Override
  @Nonnull
  public String getAspectName() {
    return DATAHUB_VIEW_INFO_ASPECT_NAME;
  }
}
