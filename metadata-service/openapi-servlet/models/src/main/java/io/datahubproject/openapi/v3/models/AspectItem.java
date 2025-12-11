/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.v3.models;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class AspectItem {
  RecordTemplate aspect;
  SystemMetadata systemMetadata;
  AuditStamp auditStamp;
}
