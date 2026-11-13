package com.linkedin.metadata.aspect.hooks.migrations.criterion;

import static com.linkedin.metadata.Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME;

import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

/**
 * v1 → v2: strips legacy {@code value} from {@code filter} criteria (see
 * com.linkedin.metadata.query.filter.Criterion).
 */
@Component
public class DynamicFormCriterionMutator extends CriterionFilterMutatorBase {

  @Override
  @Nonnull
  public String getAspectName() {
    return DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME;
  }
}
