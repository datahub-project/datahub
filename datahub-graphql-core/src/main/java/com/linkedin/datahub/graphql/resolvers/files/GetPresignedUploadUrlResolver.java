package com.linkedin.datahub.graphql.resolvers.files;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlInput;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlResponse;
import com.linkedin.datahub.graphql.generated.UploadDownloadScenario;
import com.linkedin.datahub.graphql.resolvers.mutate.DescriptionUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LinkUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageReference;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GetPresignedUploadUrlResolver
    implements DataFetcher<CompletableFuture<GetPresignedUploadUrlResponse>> {

  private final ObjectStorageClient objectStorageClient;
  private final ObjectStorageConfiguration objectStorageConfiguration;
  private final EntityClient _entityClient;

  public GetPresignedUploadUrlResolver(
      @Nullable ObjectStorageClient objectStorageClient,
      ObjectStorageConfiguration objectStorageConfiguration,
      EntityClient entityClient) {
    this.objectStorageClient = objectStorageClient;
    this.objectStorageConfiguration = objectStorageConfiguration;
    this._entityClient = entityClient;
  }

  @Override
  public CompletableFuture<GetPresignedUploadUrlResponse> get(DataFetchingEnvironment environment)
      throws Exception {
    if (objectStorageClient == null || !objectStorageClient.isConfigured()) {
      throw new IllegalArgumentException("Object storage is not configured");
    }
    if (!objectStorageClient.supportsPresignedUrls()) {
      throw new IllegalArgumentException(
          "Presigned upload URLs are not supported for provider " + objectStorageClient.provider());
    }

    String bucketName = objectStorageClient.storageBucket();
    if (bucketName == null || bucketName.isEmpty()) {
      throw new IllegalArgumentException("Object storage bucket is not configured");
    }

    final GetPresignedUploadUrlInput input =
        bindArgument(environment.getArgument("input"), GetPresignedUploadUrlInput.class);

    final QueryContext context = environment.getContext();

    validateInput(context, input);

    String newFileId = generateNewFileId(input);
    String objectKey = getObjectKey(input, newFileId);
    String contentType = input.getContentType();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          ObjectStorageReference ref = new ObjectStorageReference(bucketName, objectKey);
          String presignedUploadUrl =
              objectStorageClient.presignedUploadUrl(
                  ref,
                  objectStorageConfiguration.getPresignedUploadUrlExpirationSeconds(),
                  contentType);

          GetPresignedUploadUrlResponse result = new GetPresignedUploadUrlResponse();
          result.setUrl(presignedUploadUrl);
          result.setFileId(newFileId);
          return result;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateInput(final QueryContext context, final GetPresignedUploadUrlInput input) {
    UploadDownloadScenario scenario = input.getScenario();

    if (scenario == UploadDownloadScenario.ASSET_DOCUMENTATION) {
      validateInputForAssetDocumentationScenario(context, input);
    }

    if (scenario == UploadDownloadScenario.ASSET_DOCUMENTATION_LINKS) {
      validateInputForAssetDocumentationLinksScenario(context, input);
    }
  }

  private void validateInputForAssetDocumentationScenario(
      final QueryContext context, final GetPresignedUploadUrlInput input) {
    String assetUrn = input.getAssetUrn();
    String schemaFieldUrn = input.getSchemaFieldUrn();

    if (assetUrn == null) {
      throw new IllegalArgumentException("assetUrn is required for ASSET_DOCUMENTATION scenario");
    }

    if (schemaFieldUrn != null) {
      if (!DescriptionUtils.isAuthorizedToUpdateFieldDescription(
          context, UrnUtils.getUrn(assetUrn))) {
        throw new AuthorizationException(
            "Unauthorized to edit documentation for schema field: " + schemaFieldUrn);
      }
    } else {
      if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, UrnUtils.getUrn(assetUrn))) {
        throw new AuthorizationException(
            "Unauthorized to edit documentation for asset: " + assetUrn);
      }
    }
  }

  private void validateInputForAssetDocumentationLinksScenario(
      final QueryContext context, final GetPresignedUploadUrlInput input) {
    String assetUrnString = input.getAssetUrn();

    if (assetUrnString == null) {
      throw new IllegalArgumentException("assetUrn is required for ASSET_DOCUMENTATION scenario");
    }

    Urn assetUrn = UrnUtils.getUrn(assetUrnString);
    if (!LinkUtils.isAuthorizedToUpdateLinks(context, assetUrn)
        && !GlossaryUtils.canUpdateGlossaryEntity(assetUrn, context, _entityClient)) {
      throw new AuthorizationException("Unauthorized to edit links for asset: " + assetUrnString);
    }
  }

  private String generateNewFileId(final GetPresignedUploadUrlInput input) {
    return String.format(
        "%s%s%s",
        UUID.randomUUID().toString(), Constants.S3_FILE_ID_NAME_SEPARATOR, input.getFileName());
  }

  private String getObjectKey(final GetPresignedUploadUrlInput input, final String fileId) {
    UploadDownloadScenario scenario = input.getScenario();

    if (scenario == UploadDownloadScenario.ASSET_DOCUMENTATION
        || scenario == UploadDownloadScenario.ASSET_DOCUMENTATION_LINKS) {
      return String.format("%s/%s", objectStorageConfiguration.getAssetPathPrefix(), fileId);
    }

    throw new IllegalArgumentException("Unsupported upload scenario: " + scenario);
  }
}
