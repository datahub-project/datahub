package com.linkedin.metadata.service.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DynamicFormAssignment;
import io.datahubproject.metadata.context.OperationContext;
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
  OperationContext opContext;
  DynamicFormAssignment formFilters;
  Urn formUrn;
  int batchFormEntityCount;
  SystemEntityClient entityClient;
}
