package com.linkedin.metadata.search.cache;

import static com.datahub.util.RecordUtils.*;
import static com.linkedin.metadata.search.utils.GZIPUtil.*;

import com.linkedin.metadata.graph.EntityLineageResult;
import java.io.Serializable;
import lombok.Data;

@Data
public class CachedEntityLineageResult implements Serializable {
  private final byte[] entityLineageResult;
  private final long timestamp;

  public CachedEntityLineageResult(EntityLineageResult lineageResult, long timestamp) {
    this.entityLineageResult = gzipCompress(toJsonString(lineageResult));
    this.timestamp = timestamp;
  }

  public EntityLineageResult getEntityLineageResult() {
    return toRecordTemplate(EntityLineageResult.class, gzipDecompress(entityLineageResult));
  }
}
