package com.linkedin.metadata.systemmetadata;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.mxe.SystemMetadata;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.client.tasks.GetTaskResponse;

public interface SystemMetadataService {
  /**
   * Deletes a specific aspect from the system metadata service.
   *
   * @param urn the urn of the entity
   * @param aspect the aspect to delete
   */
  void deleteAspect(String urn, String aspect);

  Optional<GetTaskResponse> getTaskStatus(@Nonnull String nodeId, long taskId);

  void deleteUrn(String finalOldUrn);

  void setDocStatus(String urn, boolean removed);

  void insert(@Nullable SystemMetadata systemMetadata, String urn, String aspect);

  List<AspectRowSummary> findByRunId(String runId, boolean includeSoftDeleted, int from, int size);

  List<AspectRowSummary> findByUrn(String urn, boolean includeSoftDeleted, int from, int size);

  List<AspectRowSummary> findByParams(
      Map<String, String> systemMetaParams, boolean includeSoftDeleted, int from, int size);

  List<AspectRowSummary> findByRegistry(
      String registryName, String registryVersion, boolean includeSoftDeleted, int from, int size);

  List<IngestionRunSummary> listRuns(
      Integer pageOffset, Integer pageSize, boolean includeSoftDeleted);

  List<AspectRowSummary> findAspectsByUrn(
      @Nonnull Urn urn, @Nonnull List<String> aspects, boolean includeSoftDeleted);

  default void configure() {}

  void clear();
}
