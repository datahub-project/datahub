/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.v3.models;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class SortCriterion {
  private String field;
  private SortOrder order;

  public enum SortOrder {
    ASCENDING,
    DESCENDING
  }

  /** Convert this SortCriterion to its counterpart in RecordTemplate. */
  @Nonnull
  public com.linkedin.metadata.query.filter.SortCriterion toRecordTemplate() {
    com.linkedin.metadata.query.filter.SortCriterion sortCriterion =
        new com.linkedin.metadata.query.filter.SortCriterion();
    sortCriterion.setField(this.field);

    // By default, sort in ASCENDING order.
    sortCriterion.setOrder(com.linkedin.metadata.query.filter.SortOrder.ASCENDING);

    if (this.order != null) {
      sortCriterion.setOrder(
          com.linkedin.metadata.query.filter.SortOrder.valueOf(this.order.name()));
    }

    return sortCriterion;
  }
}
