/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.v3.models;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class GenericEntityAspectsBodyV3 {
  @Nullable private Set<String> entities;
  @Nullable private Set<String> aspects;
  @Nullable private Filter filter;
  @Nullable private List<SortCriterion> sortCriteria;
}
