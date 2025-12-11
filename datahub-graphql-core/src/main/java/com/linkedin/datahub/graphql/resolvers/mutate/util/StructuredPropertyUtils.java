/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.structured.PrimitivePropertyValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StructuredPropertyUtils {

  private StructuredPropertyUtils() {}

  @Nullable
  public static PrimitivePropertyValue mapPropertyValueInput(
      @Nonnull final PropertyValueInput valueInput) {
    if (valueInput.getStringValue() != null) {
      return PrimitivePropertyValue.create(valueInput.getStringValue());
    } else if (valueInput.getNumberValue() != null) {
      return PrimitivePropertyValue.create(valueInput.getNumberValue().doubleValue());
    }
    return null;
  }
}
