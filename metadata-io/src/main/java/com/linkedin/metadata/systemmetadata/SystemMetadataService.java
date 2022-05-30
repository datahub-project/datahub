package com.linkedin.metadata.systemmetadata;

import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.mxe.SystemMetadata;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


public interface SystemMetadataService {
  /**
   * Deletes a specific aspect from the system metadata service.
   *
   * @param urn the urn of the entity
   * @param aspect the aspect to delete
   */
  void deleteAspect(String urn, String aspect);

  void deleteUrn(String finalOldUrn);

  void setDocStatus(String urn, boolean removed);

  void insert(@Nullable SystemMetadata systemMetadata, String urn, String aspect);

  List<AspectRowSummary> findByRunId(String runId, boolean includeSoftDeleted, int from, int size);

  List<AspectRowSummary> findByUrn(String urn, boolean includeSoftDeleted, int from, int size);

  List<AspectRowSummary> findByParams(Map<String, String> systemMetaParams, boolean includeSoftDeleted, int from, int size);

  List<AspectRowSummary> findByRegistry(String registryName, String registryVersion, boolean includeSoftDeleted, int from, int size);

  List<IngestionRunSummary> listRuns(Integer pageOffset, Integer pageSize, boolean includeSoftDeleted);

  void configure();

  void clear();
}
