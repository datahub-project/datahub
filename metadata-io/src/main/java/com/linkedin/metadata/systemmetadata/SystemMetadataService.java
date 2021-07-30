package com.linkedin.metadata.systemmetadata;

import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.mxe.SystemMetadata;
import java.util.List;


public interface SystemMetadataService {
  Boolean delete(String urn, String aspect);

  void deleteUrn(String finalOldUrn);

  void insert(SystemMetadata systemMetadata, String urn, String aspect);

  List<AspectRowSummary> findByRunId(String runId);

  List<IngestionRunSummary> listRuns(
      final Integer pageOffset,
      final Integer pageSize);

  void configure();

  void clear();
}
