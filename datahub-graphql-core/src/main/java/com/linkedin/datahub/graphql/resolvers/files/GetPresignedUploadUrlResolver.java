package com.linkedin.datahub.graphql.resolvers.files;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlInput;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlResponse;
import com.linkedin.datahub.graphql.generated.UploadDownloadScenario;
import com.linkedin.datahub.graphql.resolvers.mutate.DescriptionUtils;
import com.linkedin.metadata.config.S3Configuration;
import com.linkedin.metadata.utils.aws.S3Util;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GetPresignedUploadUrlResolver
    implements DataFetcher<CompletableFuture<GetPresignedUploadUrlResponse>> {

  private final S3Util s3Util;
  private final S3Configuration s3Configuration;

  public GetPresignedUploadUrlResolver(S3Util s3Util, S3Configuration s3Configuration) {
    this.s3Util = s3Util;
    this.s3Configuration = s3Configuration;
  }

  @Override
  public CompletableFuture<GetPresignedUploadUrlResponse> get(DataFetchingEnvironment environment)
      throws Exception {
    if (s3Util == null) {
      throw new IllegalArgumentException("S3Util isn't provided");
    }

    String bucketName = s3Configuration.getBucketName();

    if (bucketName == null || bucketName.isEmpty()) {
      throw new IllegalArgumentException("Bucket name isn't provided");
    }

    final GetPresignedUploadUrlInput input =
        bindArgument(environment.getArgument("input"), GetPresignedUploadUrlInput.class);

    final QueryContext context = environment.getContext();

    validateInput(context, input);

    String newFileId = generateNewFileId(input);
    String s3Key = getS3Key(input, newFileId, bucketName);
    String contentType = input.getContentType();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          String presignedUploadUrl =
              s3Util.generatePresignedUploadUrl(
                  bucketName,
                  s3Key,
                  s3Configuration.getPresignedUploadUrlExpirationSeconds(),
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
  }

  private void validateInputForAssetDocumentationScenario(
      final QueryContext context, final GetPresignedUploadUrlInput input) {
    String assetUrn = input.getAssetUrn();
    String schemaFieldUrn = input.getSchemaFieldUrn();

    if (assetUrn == null) {
      throw new IllegalArgumentException("assetUrn is required for ASSET_DOCUMENTATION scenario");
    }

    // FYI: for schema field we have to apply another rules to check permissions
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

  private String generateNewFileId(final GetPresignedUploadUrlInput input) {
    return String.format("%s-%s", UUID.randomUUID().toString(), input.getFileName());
  }

  private String getS3Key(
      final GetPresignedUploadUrlInput input, final String fileId, final String bucketName) {
    UploadDownloadScenario scenario = input.getScenario();

    if (scenario == UploadDownloadScenario.ASSET_DOCUMENTATION) {
      return String.format(
          "%s/%s",
          s3Configuration.getAssetPathPrefix(), fileId);
    } else {
      throw new IllegalArgumentException("Unsupported upload scenario: " + scenario);
    }
  }
}
