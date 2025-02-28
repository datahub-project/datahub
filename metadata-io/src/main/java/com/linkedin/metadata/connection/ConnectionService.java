package com.linkedin.metadata.connection;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.connection.DataHubConnectionDetails;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DataHubConnectionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ConnectionService {

  private final EntityClient _entityClient;

  /**
   * Upserts a DataHub connection. If the connection with the provided ID already exists, then it
   * will be overwritten.
   *
   * <p>This method assumes that authorization has already been verified at the calling layer.
   *
   * @return the URN of the new connection.
   */
  public Urn upsertConnection(
      @Nonnull OperationContext opContext,
      @Nullable final String id,
      @Nonnull final Urn platformUrn,
      @Nonnull final DataHubConnectionDetailsType type,
      @Nullable final DataHubJsonConnection json,
      @Nullable final String name) {
    Objects.requireNonNull(platformUrn, "platformUrn must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");

    // 1. Optionally generate new connection id
    final String connectionId = id != null ? id : UUID.randomUUID().toString();
    final DataHubConnectionKey key = new DataHubConnectionKey().setId(connectionId);
    final Urn connectionUrn =
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_CONNECTION_ENTITY_NAME);

    // 2. Build Connection Details
    final DataHubConnectionDetails details = new DataHubConnectionDetails();
    details.setType(type);
    // default set name as ID if it exists, otherwise use name if it exists
    details.setName(id, SetMode.IGNORE_NULL);
    details.setName(name, SetMode.IGNORE_NULL);

    if (DataHubConnectionDetailsType.JSON.equals(details.getType())) {
      if (json != null) {
        details.setJson(json);
      } else {
        throw new IllegalArgumentException(
            "Connections with type JSON must provide the field 'json'.");
      }
    }

    // 3. Build platform instance
    final DataPlatformInstance platformInstance = new DataPlatformInstance();
    platformInstance.setPlatform(platformUrn);

    // 4. Write changes to GMS
    try {
      final List<MetadataChangeProposal> aspectsToIngest = new ArrayList<>();
      aspectsToIngest.add(
          AspectUtils.buildMetadataChangeProposal(
              connectionUrn, Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME, details));
      aspectsToIngest.add(
          AspectUtils.buildMetadataChangeProposal(
              connectionUrn, Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME, platformInstance));
      _entityClient.batchIngestProposals(opContext, aspectsToIngest, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to upsert Connection with urn %s", connectionUrn), e);
    }
    return connectionUrn;
  }

  @Nullable
  public DataHubConnectionDetails getConnectionDetails(
      @Nonnull OperationContext opContext, @Nonnull final Urn connectionUrn) {
    Objects.requireNonNull(connectionUrn, "connectionUrn must not be null");
    final EntityResponse response = getConnectionEntityResponse(opContext, connectionUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME)) {
      return new DataHubConnectionDetails(
          response
              .getAspects()
              .get(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME)
              .getValue()
              .data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  public EntityResponse getConnectionEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn connectionUrn) {
    try {
      return _entityClient.getV2(
          opContext,
          Constants.DATAHUB_CONNECTION_ENTITY_NAME,
          connectionUrn,
          ImmutableSet.of(
              Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
              Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
              Constants.STATUS_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Connection with urn %s", connectionUrn), e);
    }
  }

  /**
   * Upserts a DataHub connection. If the connection with the provided ID already exists, then it
   * will be overwritten.
   *
   * <p>This method assumes that authorization has already been verified at the calling layer.
   *
   * @return the URN of the new connection.
   */
  public Urn updateConnection(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn connectionUrn,
      @Nullable final Urn platformUrn,
      @Nullable final DataHubConnectionDetailsType type,
      @Nullable final DataHubJsonConnection json,
      @Nullable final String name) {
    Objects.requireNonNull(connectionUrn, "urn must not be null");
    // 1. Get existing connection details
    final DataHubConnectionDetails details = getConnectionDetails(opContext, connectionUrn);

    if (details == null) {
      throw new IllegalArgumentException(
          String.format("Connection with urn %s does not exist", connectionUrn));
    }

    // 2. Update details according to input
    if (name != null) {
      details.setName(name);
    }
    if (type != null) {
      details.setType(type);
    }
    if (json != null) {
      details.setJson(json);
    }

    if (DataHubConnectionDetailsType.JSON.equals(details.getType())) {
      if (details.getJson() == null) {
        throw new IllegalArgumentException(
            "Connections with type JSON must provide the field 'json'.");
      }
    }

    final List<MetadataChangeProposal> aspectsToIngest = new ArrayList<>();

    // 3. Build platform instance if exists
    if (platformUrn != null) {
      final DataPlatformInstance platformInstance = new DataPlatformInstance();
      platformInstance.setPlatform(platformUrn);
      aspectsToIngest.add(
          AspectUtils.buildMetadataChangeProposal(
              connectionUrn, Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME, platformInstance));
    }

    // 4. Write changes to GMS
    aspectsToIngest.add(
        AspectUtils.buildMetadataChangeProposal(
            connectionUrn, Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME, details));
    try {
      _entityClient.batchIngestProposals(opContext, aspectsToIngest, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update Connection with urn %s", connectionUrn), e);
    }
    return connectionUrn;
  }

  public boolean deleteConnection(@Nonnull OperationContext opContext, @Nonnull Urn connectionUrn)
      throws RemoteInvocationException {
    _entityClient.deleteEntity(opContext, connectionUrn);
    CompletableFuture.runAsync(
        () -> {
          try {
            _entityClient.deleteEntityReferences(opContext, connectionUrn);
          } catch (RemoteInvocationException e) {
            log.error(
                String.format(
                    "Caught exception while attempting to clear all entity references for Connection with urn %s",
                    connectionUrn),
                e);
          }
        });
    return true;
  }

  public boolean softDeleteConnection(
      @Nonnull OperationContext opContext, @Nonnull Urn connectionUrn)
      throws RemoteInvocationException {
    Status status = new Status();

    final EntityResponse response = getConnectionEntityResponse(opContext, connectionUrn);
    if (response != null && response.getAspects().containsKey(Constants.STATUS_ASPECT_NAME)) {
      status =
          new Status(response.getAspects().get(Constants.STATUS_ASPECT_NAME).getValue().data());
    }
    status.setRemoved(true);
    MetadataChangeProposal mcp =
        AspectUtils.buildMetadataChangeProposal(
            connectionUrn, Constants.STATUS_ASPECT_NAME, status);

    _entityClient.ingestProposal(opContext, mcp, false);
    return true;
  }
}
