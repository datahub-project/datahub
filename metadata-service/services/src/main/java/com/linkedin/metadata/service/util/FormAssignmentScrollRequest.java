package com.linkedin.metadata.service.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DynamicFormAssignment;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/**
 * Groups the parameters for {@link
 * SearchBasedFormAssignmentManager#apply(FormAssignmentScrollRequest)} so callers pass one argument
 * instead of five positional ones.
 */
@Builder
@Value
public class FormAssignmentScrollRequest {
  @Nonnull OperationContext opContext;
  @Nonnull DynamicFormAssignment formFilters;
  @Nonnull Urn formUrn;
  int batchFormEntityCount;
  @Nonnull SystemEntityClient entityClient;
}
