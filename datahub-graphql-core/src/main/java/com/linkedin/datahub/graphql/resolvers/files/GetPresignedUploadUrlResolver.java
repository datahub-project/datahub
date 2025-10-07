package com.linkedin.datahub.graphql.resolvers.files;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlInput;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlResponse;
import com.linkedin.datahub.graphql.generated.UPLOAD_DOWNLOAD_SCENARIO;
import com.linkedin.datahub.graphql.resolvers.mutate.DescriptionUtils;
import com.linkedin.datahub.graphql.util.S3Util;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.common.urn.Urn;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

@Slf4j
@RequiredArgsConstructor
@Component
public class GetPresignedUploadUrlResolver
    implements DataFetcher<CompletableFuture<GetPresignedUploadUrlResponse>> {

  private static final int EXPIRATION_SECONDS = 60 * 60; // 60 minutes

  private final S3Util s3Util;

  @Override
  public CompletableFuture<GetPresignedUploadUrlResponse> get(DataFetchingEnvironment environment)
      throws Exception {
    final GetPresignedUploadUrlInput input =
        bindArgument(environment.getArgument("input"), GetPresignedUploadUrlInput.class);

    final QueryContext context = environment.getContext();

    validateInput(context, input);

    String s3Key = getS3Key(input);
    String contentType = "application/octet-stream"; // Default content type

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          String presignedUploadUrl =
              s3Util.generatePresignedUploadUrl("test", s3Key, EXPIRATION_SECONDS, contentType);

          GetPresignedUploadUrlResponse result = new GetPresignedUploadUrlResponse();
          result.setUrl(presignedUploadUrl);
          return result;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateInput(final QueryContext context, final GetPresignedUploadUrlInput input) {
    UPLOAD_DOWNLOAD_SCENARIO scenario = input.getScenario();

    if (Objects.requireNonNull(scenario) == UPLOAD_DOWNLOAD_SCENARIO.ASSET_DOCUMENTATION) {
      validateInputForAssetDocumentationScenario(context, input);
    }
  }

  private void validateInputForAssetDocumentationScenario(
      final QueryContext context, final GetPresignedUploadUrlInput input) {
    String assetUrn = input.getAssetUrn();

    if (assetUrn == null) {
      throw new IllegalArgumentException("assetUrn is required for ASSET_DOCUMENTATION scenario");
    }

    if (!DescriptionUtils.isAuthorizedToUpdateDescription(context, UrnUtils.getUrn(assetUrn))) {
      throw new AuthorizationException("Unauthorized to edit documentation for asset: " + assetUrn);
    }
  }

  private String getS3Key(final GetPresignedUploadUrlInput input) {
    UPLOAD_DOWNLOAD_SCENARIO scenario = input.getScenario();
    String fileId = input.getFileId();

    if (Objects.requireNonNull(scenario) == UPLOAD_DOWNLOAD_SCENARIO.ASSET_DOCUMENTATION) {
      return String.format("%s/%s", "test", fileId);
    } else {
      throw new IllegalArgumentException("Unsupported upload scenario: " + scenario);
    }
  }
}
