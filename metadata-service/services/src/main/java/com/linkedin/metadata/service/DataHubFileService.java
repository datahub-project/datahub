package com.linkedin.metadata.service;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.file.DataHubFileInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DataHubFileKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataHubFileService {
  private final EntityClient entityClient;

  public DataHubFileService(@Nonnull EntityClient entityClient) {
    this.entityClient = entityClient;
  }

  /**
   * Creates a new DataHub file entity. If a file with the provided id already exists, an exception
   * will be thrown.
   *
   * <p>This method assumes that authorization has already been verified at the calling layer.
   *
   * @return the URN of the new file.
   */
  public Urn createDataHubFile(
      @Nonnull OperationContext opContext,
      @Nonnull final String id,
      @Nonnull final String storageBucket,
      @Nonnull final String storageKey,
      @Nonnull final String originalFileName,
      @Nonnull final String mimeType,
      @Nonnull final Long sizeInBytes,
      @Nonnull final com.linkedin.file.FileUploadScenario scenario,
      @Nullable final Urn referencedByAsset,
      @Nullable final Urn schemaField,
      @Nullable final String contentHash) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(storageBucket, "storageBucket must not be null");
    Objects.requireNonNull(storageKey, "storageKey must not be null");
    Objects.requireNonNull(originalFileName, "originalFileName must not be null");
    Objects.requireNonNull(mimeType, "mimeType must not be null");
    Objects.requireNonNull(sizeInBytes, "sizeInBytes must not be null");
    Objects.requireNonNull(scenario, "scenario must not be null");

    // 1. Create file urn from id
    final DataHubFileKey key = new DataHubFileKey().setId(id);
    final Urn fileUrn =
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_FILE_ENTITY_NAME);

    // 2. Check if file already exists
    if (fileExists(opContext, fileUrn)) {
      throw new RuntimeException(
          String.format("DataHubFile with id %s already exists. URN: %s", id, fileUrn));
    }

    final AuditStamp nowAuditStamp = opContext.getAuditStamp();

    // 3. Build DataHubFileInfo
    DataHubFileInfo fileInfo = new DataHubFileInfo();
    fileInfo.setStorageBucket(storageBucket);
    fileInfo.setStorageKey(storageKey);
    fileInfo.setOriginalFileName(originalFileName);
    fileInfo.setMimeType(mimeType);
    fileInfo.setSizeInBytes(sizeInBytes);
    fileInfo.setScenario(scenario);
    fileInfo.setCreated(nowAuditStamp);

    if (referencedByAsset != null) {
      fileInfo.setReferencedByAsset(referencedByAsset);
    }

    if (schemaField != null) {
      fileInfo.setSchemaField(schemaField);
    }

    if (contentHash != null) {
      fileInfo.setContentHash(contentHash);
    }

    // 4. Write changes to GMS
    try {
      final List<MetadataChangeProposal> aspectsToIngest = new ArrayList<>();
      aspectsToIngest.add(
          AspectUtils.buildMetadataChangeProposal(
              fileUrn, Constants.DATAHUB_FILE_INFO_ASPECT_NAME, fileInfo));
      entityClient.batchIngestProposals(opContext, aspectsToIngest, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create DataHubFile with urn %s", fileUrn), e);
    }
    return fileUrn;
  }

  /**
   * Checks if a DataHubFile entity exists.
   *
   * @param opContext the operation context
   * @param fileUrn the URN of the file to check
   * @return true if the file exists, false otherwise
   */
  public boolean fileExists(@Nonnull OperationContext opContext, @Nonnull final Urn fileUrn) {
    Objects.requireNonNull(fileUrn, "fileUrn must not be null");
    try {
      return entityClient.exists(opContext, fileUrn);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to check if DataHubFile exists with urn %s", fileUrn), e);
    }
  }

  /**
   * Gets the DataHubFileInfo aspect for a given file URN.
   *
   * @param opContext the operation context
   * @param fileUrn the URN of the file
   * @return the DataHubFileInfo aspect, or null if not found
   */
  @Nullable
  public DataHubFileInfo getDataHubFileInfo(
      @Nonnull OperationContext opContext, @Nonnull final Urn fileUrn) {
    Objects.requireNonNull(fileUrn, "fileUrn must not be null");
    final EntityResponse response = getDataHubFileEntityResponse(opContext, fileUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)) {
      return new DataHubFileInfo(
          response.getAspects().get(Constants.DATAHUB_FILE_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Gets the full entity response for a DataHubFile.
   *
   * @param opContext the operation context
   * @param fileUrn the URN of the file
   * @return the EntityResponse, or null if not found
   */
  @Nullable
  public EntityResponse getDataHubFileEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn fileUrn) {
    try {
      return entityClient.getV2(
          opContext, Constants.DATAHUB_FILE_ENTITY_NAME, fileUrn, null, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve DataHubFile with urn %s", fileUrn), e);
    }
  }

  /**
   * Deletes a DataHub file.
   *
   * @param opContext the operation context
   * @param fileUrn the URN of the file to delete
   */
  public void deleteDataHubFile(@Nonnull OperationContext opContext, @Nonnull final Urn fileUrn) {
    Objects.requireNonNull(fileUrn, "fileUrn must not be null");

    try {
      entityClient.deleteEntity(opContext, fileUrn);
      log.info(String.format("Successfully deleted DataHubFile with urn %s", fileUrn));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to delete DataHubFile with urn %s", fileUrn), e);
    }
  }
}
