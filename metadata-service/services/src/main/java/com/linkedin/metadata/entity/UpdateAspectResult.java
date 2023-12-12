package com.linkedin.metadata.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.transactions.AbstractBatchItem;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.SystemMetadata;
import java.util.concurrent.Future;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class UpdateAspectResult {
  Urn urn;
  AbstractBatchItem request;
  RecordTemplate oldValue;
  RecordTemplate newValue;
  SystemMetadata oldSystemMetadata;
  SystemMetadata newSystemMetadata;
  MetadataAuditOperation operation;
  AuditStamp auditStamp;
  long maxVersion;
  boolean processedMCL;
  Future<?> mclFuture;
}
