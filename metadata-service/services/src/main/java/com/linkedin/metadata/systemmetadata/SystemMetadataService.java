package com.linkedin.metadata.systemmetadata;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.client.tasks.GetTaskResponse;

public interface SystemMetadataService {

  SystemMetadataServiceConfig getSystemMetadataServiceConfig();

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

  List<AspectRowSummary> findByRunId(
      String runId, boolean includeSoftDeleted, int from, @Nullable Integer size);

  List<AspectRowSummary> findByUrn(
      String urn, boolean includeSoftDeleted, int from, @Nullable Integer size);

  List<AspectRowSummary> findByParams(
      Map<String, String> systemMetaParams,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size);

  List<AspectRowSummary> findByRegistry(
      String registryName,
      String registryVersion,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size);

  List<IngestionRunSummary> listRuns(
      Integer pageOffset, Integer pageSize, boolean includeSoftDeleted);

  List<AspectRowSummary> findAspectsByUrn(
      @Nonnull Urn urn, @Nonnull List<String> aspects, boolean includeSoftDeleted);

  default void configure() {}

  void clear();

  /**
   * Returns raw elasticsearch documents
   *
   * @param opContext operation context
   * @param urnAspects the map of urns to aspect names
   * @return map of urns, aspect names, to raw elasticsearch documents
   */
  Map<Urn, Map<String, Map<String, Object>>> raw(
      OperationContext opContext, Map<String, Set<String>> urnAspects);
}
