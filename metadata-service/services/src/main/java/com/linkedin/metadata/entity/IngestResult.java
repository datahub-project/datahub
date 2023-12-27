package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.transactions.AbstractBatchItem;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class IngestResult {
  Urn urn;
  AbstractBatchItem request;
  boolean publishedMCL;
  boolean processedMCL;
  boolean publishedMCP;
  boolean sqlCommitted;
  boolean isUpdate; // update else insert
}
