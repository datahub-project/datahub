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
   * @param opContext per-event operation context
   * @param urn the urn of the entity
   * @param aspect the aspect to delete
   */
  void deleteAspect(@Nonnull OperationContext opContext, String urn, String aspect);

  Optional<GetTaskResponse> getTaskStatus(
      @Nonnull OperationContext opContext, @Nonnull String nodeId, long taskId);

  void deleteUrn(@Nonnull OperationContext opContext, String finalOldUrn);

  void setDocStatus(@Nonnull OperationContext opContext, String urn, boolean removed);

  void insert(
      @Nonnull OperationContext opContext,
      @Nullable SystemMetadata systemMetadata,
      String urn,
      String aspect);

  List<AspectRowSummary> findByRunId(
      @Nonnull OperationContext opContext,
      String runId,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size);

  List<AspectRowSummary> findByUrn(
      @Nonnull OperationContext opContext,
      String urn,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size);

  List<AspectRowSummary> findByParams(
      @Nonnull OperationContext opContext,
      Map<String, String> systemMetaParams,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size);

  List<AspectRowSummary> findByRegistry(
      @Nonnull OperationContext opContext,
      String registryName,
      String registryVersion,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size);

  List<IngestionRunSummary> listRuns(
      @Nonnull OperationContext opContext,
      Integer pageOffset,
      Integer pageSize,
      boolean includeSoftDeleted);

  List<AspectRowSummary> findAspectsByUrn(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<String> aspects,
      boolean includeSoftDeleted);

  default void configure() {}

  void clear(@Nonnull OperationContext opContext);

  /**
   * Returns raw elasticsearch documents
   *
   * @param opContext operation context
   * @param urnAspects the map of urns to aspect names
   * @return map of urns, aspect names, to raw elasticsearch documents
   */
  Map<Urn, Map<String, Map<String, Object>>> raw(
      OperationContext opContext, Map<String, Set<String>> urnAspects);

  /**
   * Count entities for a single registry key aspect, split by active vs soft-deleted system
   * metadata documents.
   */
  @Nonnull
  KeyAspectCount countByKeyAspect(
      @Nonnull OperationContext opContext, @Nonnull String keyAspectName);

  /**
   * Count entities for multiple registry key aspects in one query, split by active vs soft-deleted.
   */
  @Nonnull
  Map<String, KeyAspectCount> countByKeyAspects(
      @Nonnull OperationContext opContext, @Nonnull List<String> keyAspectNames);
}
