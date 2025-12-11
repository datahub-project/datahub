/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.Value;

@Value
public class RelationshipFieldSpec implements FieldSpec {

  @NonNull PathSpec path;
  @NonNull RelationshipAnnotation relationshipAnnotation;
  @NonNull DataSchema pegasusSchema;

  /** Returns the name of the outbound relationship extending from the field. */
  @Nonnull
  public String getRelationshipName() {
    return relationshipAnnotation.getName();
  }

  /** Returns a list of entity names representing the destination node type of the relationship. */
  @Nonnull
  public List<String> getValidDestinationTypes() {
    return relationshipAnnotation.getValidDestinationTypes();
  }

  public boolean isLineageRelationship() {
    return relationshipAnnotation.isLineage();
  }
}
