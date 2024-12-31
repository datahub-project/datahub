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
