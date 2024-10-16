package com.linkedin.metadata.entity;

import static com.linkedin.metadata.utils.PegasusUtils.constructMCL;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class UpdateAspectResult {
  Urn urn;
  ChangeMCP request;
  @Nullable RecordTemplate oldValue;
  RecordTemplate newValue;
  @Nullable SystemMetadata oldSystemMetadata;
  SystemMetadata newSystemMetadata;
  MetadataAuditOperation operation;
  AuditStamp auditStamp;
  long maxVersion;
  @Nullable MetadataChangeProposal mcp;
  /*
   Whether the MCL was written to Elasticsearch prior to emitting the MCL
  */
  boolean processedMCL;
  Future<?> mclFuture;

  public boolean isNoOp() {
    return oldValue == newValue;
  }

  public MetadataChangeLog toMCL() {
    return constructMCL(
        request.getMetadataChangeProposal(),
        urnToEntityName(urn),
        urn,
        isNoOp() ? ChangeType.RESTATE : ChangeType.UPSERT,
        request.getAspectName(),
        auditStamp,
        newValue,
        newSystemMetadata,
        oldValue,
        oldSystemMetadata);
  }
}
