/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.v3.models;

import com.linkedin.metadata.query.filter.CriterionArray;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class ConjunctiveCriterion {

  // A list criterion for the filter. (AND operation to combine - Criterion AND Criterion)
  private List<Criterion> criteria;

  /** Convert this ConjunctiveCriterion to its counterpart in RecordTemplate. */
  @Nullable
  public com.linkedin.metadata.query.filter.ConjunctiveCriterion toRecordTemplate() {
    if (criteria == null) return null;

    com.linkedin.metadata.query.filter.ConjunctiveCriterion conjunctiveCriterion =
        new com.linkedin.metadata.query.filter.ConjunctiveCriterion();

    CriterionArray criterionArray = new CriterionArray();

    for (Criterion criterion : criteria) {
      criterionArray.add(criterion.toRecordTemplate());
    }

    conjunctiveCriterion.setAnd(criterionArray);
    return conjunctiveCriterion;
  }
}
