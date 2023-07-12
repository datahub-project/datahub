package com.linkedin.metadata.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.SystemMetadata;
import lombok.Value;


@Value
public class UpdateAspectResult {
  Urn urn;
  RecordTemplate oldValue;
  RecordTemplate newValue;
  SystemMetadata oldSystemMetadata;
  SystemMetadata newSystemMetadata;
  MetadataAuditOperation operation;
  AuditStamp auditStamp;
  long maxVersion;
}
