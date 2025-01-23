package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.batch.BatchItem;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class IngestResult {
  Urn urn;
  BatchItem request;
  @Nullable UpdateAspectResult result;
  boolean publishedMCL;
  boolean processedMCL;
  boolean publishedMCP;
  boolean sqlCommitted;
  boolean isUpdate; // update else insert
}
