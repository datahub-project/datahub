/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.datahubusage;

import com.linkedin.metadata.datahubusage.event.EventSource;
import com.linkedin.metadata.datahubusage.event.LoginSource;
import java.util.LinkedHashMap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor(force = true, access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class InternalUsageEventResult {
  protected String eventType;
  protected long timestamp;
  protected String actorUrn;
  protected String sourceIP;
  protected EventSource eventSource;
  protected String userAgent;
  protected String telemetryTraceId;
  protected LinkedHashMap<String, Object> rawUsageEvent;

  // Subtype specific fields

  // Entity
  protected String entityUrn;
  protected String entityType;
  protected String aspectName;
  // Login
  protected LoginSource loginSource;
}
