package com.linkedin.datahub.graphql.resolvers.files;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlInput;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrl;
import com.linkedin.datahub.graphql.generated.UploadDownloadScenario;
import com.linkedin.datahub.graphql.resolvers.mutate.DescriptionUtils;
import com.linkedin.datahub.graphql.util.S3Util;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

@Slf4j
@RequiredArgsConstructor
@Component
public class GetPresignedUploadUrlResolver
    implements DataFetcher<CompletableFuture<GetPresignedUploadUrl>> {

  private static final int EXPIRATION_SECONDS = 60 * 60; // 60 minutes

  private final S3Util s3Util;

  @Override
  public CompletableFuture<GetPresignedUploadUrl> get(DataFetchingEnvironment environment)
      throws Exception {
    final GetPresignedUploadUrlInput input =
        bindArgument(environment.getArgument("input"), GetPresignedUploadUrlInput.class);

    final QueryContext context = environment.getContext();


    validateInput(context, input);

    String newFileId = UUID.randomUUID().toString();

    String s3Key = getS3Key(input, newFileId);
    String contentType = input.getContentType();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          String presignedUploadUrl =
              s3Util.generatePresignedUploadUrl("test2", s3Key, EXPIRATION_SECONDS, contentType);

          GetPresignedUploadUrl result = new GetPresignedUploadUrl();
          result.setUrl(presignedUploadUrl);
          return result;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateInput(final QueryContext context, final GetPresignedUploadUrlInput input) {
    UploadDownloadScenario scenario = input.getScenario();

    if (Objects.requireNonNull(scenario) == UploadDownloadScenario.ASSET_DOCUMENTATION) {
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

  private String getS3Key(final GetPresignedUploadUrlInput input, final String fileId) {
    UploadDownloadScenario scenario = input.getScenario();

    if (Objects.requireNonNull(scenario) == UploadDownloadScenario.ASSET_DOCUMENTATION) {
      return String.format("%s/%s", "test2", fileId);
    } else {
      throw new IllegalArgumentException("Unsupported upload scenario: " + scenario);
    }
  }
}
