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
