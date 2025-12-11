/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeline.data;

import com.linkedin.common.AuditStamp;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.NonFinal;

/** An event representing a high-level, semantic change to a DataHub entity. */
@Value
@Builder
@NonFinal
@AllArgsConstructor
public class ChangeEvent {
  /** The urn of the entity being changed. */
  String entityUrn;

  /** The category of the change. */
  ChangeCategory category;

  /** The operation of the change. */
  ChangeOperation operation;

  /** An optional modifier associated with the change. For example, a tag urn. */
  String modifier;

  /** Parameters that determined by the combination of category + operation. */
  Map<String, Object> parameters;

  /** An audit stamp detailing who made the change and when. */
  AuditStamp auditStamp;

  /** Optional: Semantic change version. TODO: Determine if this should be inside this structure. */
  SemanticChangeType semVerChange;

  /**
   * Optional: A human readable description of this change. TODO: Determine if this should be inside
   * this structure.
   */
  String description;
}
