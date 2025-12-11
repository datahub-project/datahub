/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeline.data.dataset.schema;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
@Getter
public class SchemaFieldGlossaryTermChangeEvent extends ChangeEvent {
  @Builder(builderMethodName = "schemaFieldGlossaryTermChangeEventBuilder")
  public SchemaFieldGlossaryTermChangeEvent(
      String entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      AuditStamp auditStamp,
      SemanticChangeType semVerChange,
      String description,
      String fieldPath,
      Urn parentUrn,
      Urn termUrn) {
    super(
        entityUrn,
        category,
        operation,
        modifier,
        ImmutableMap.of(
            "fieldPath", fieldPath,
            "parentUrn", parentUrn.toString(),
            "termUrn", termUrn.toString()),
        auditStamp,
        semVerChange,
        description);
  }
}
