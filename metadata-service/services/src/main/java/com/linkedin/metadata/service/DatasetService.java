package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatasetService extends BaseService {
  public DatasetService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
  }

  /**
   * Checks whether a dataset contains a particular schema field.
   *
   * @param fieldPath the field path within the schema
   * @return true if the schema field exists, false otherwise
   */
  public boolean schemaFieldExists(
      @Nonnull OperationContext opContext, @Nonnull Urn datasetUrn, @Nonnull String fieldPath)
      throws RemoteInvocationException {
    if (!Constants.DATASET_ENTITY_NAME.equals(datasetUrn.getEntityType())) {
      throw new InvalidEntityTypeException(
          String.format("Entity type %s is not a dataset", datasetUrn.getEntityType()));
    }
    if (!entityClient.exists(opContext, datasetUrn)) {
      throw new EntityDoesNotExistException(String.format("Dataset %s does not exist", datasetUrn));
    }
    final SchemaMetadata schemaMetadata = getSchemaMetadata(opContext, datasetUrn);
    if (schemaMetadata == null) {
      return false;
    }
    return schemaMetadata.getFields().stream()
        .anyMatch(field -> field.getFieldPath().equals(fieldPath));
  }

  private SchemaMetadata getSchemaMetadata(
      @Nonnull OperationContext opContext, @Nonnull Urn datasetUrn) {
    final EntityResponse response = getSchemaMetadataEntityResponse(opContext, datasetUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.SCHEMA_METADATA_ASPECT_NAME)) {
      return new SchemaMetadata(
          response.getAspects().get(Constants.SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  private EntityResponse getSchemaMetadataEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn datasetUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.DATASET_ENTITY_NAME,
          datasetUrn,
          ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve schema metadata for dataset with urn %s", datasetUrn),
          e);
    }
  }
}
