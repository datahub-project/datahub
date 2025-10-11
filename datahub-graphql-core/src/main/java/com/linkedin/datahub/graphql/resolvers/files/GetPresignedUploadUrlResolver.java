package com.linkedin.datahub.graphql.resolvers.files;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrl;
import com.linkedin.datahub.graphql.generated.GetPresignedUploadUrlInput;
import com.linkedin.datahub.graphql.generated.UploadDownloadScenario;
import com.linkedin.datahub.graphql.resolvers.mutate.DescriptionUtils;
import com.linkedin.datahub.graphql.util.S3Util;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class GetPresignedUploadUrlResolver
    implements DataFetcher<CompletableFuture<GetPresignedUploadUrl>> {

  private static final int EXPIRATION_SECONDS = 60 * 60; // 60 minutes
  private static final Set<String> ALLOWED_FILE_EXTENSIONS =
      new HashSet<>(
          Arrays.asList(
              "pdf", "jpeg", "jpg", "png", "pptx", "docx", "xls", "xml", "ppt", "gif", "xlsx",
              "bmp", "doc", "rtf", "gz", "zip", "mp4", "mp3", "wmv", "tiff", "txt", "md", "csv"));

  private final S3Util s3Util;
  private final String bucketName;

  @Override
  public CompletableFuture<GetPresignedUploadUrl> get(DataFetchingEnvironment environment)
      throws Exception {
    if (s3Util == null) {
      throw new IllegalArgumentException("S3Util isn't provided");
    }

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
              s3Util.generatePresignedUploadUrl(bucketName, s3Key, EXPIRATION_SECONDS, contentType);

          GetPresignedUploadUrl result = new GetPresignedUploadUrl();
          result.setUrl(presignedUploadUrl);
          result.setFileId(newFileId);
          return result;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateInput(final QueryContext context, final GetPresignedUploadUrlInput input) {
    UploadDownloadScenario scenario = input.getScenario();

    validateFileName(input.getFileName());

    if (scenario == UploadDownloadScenario.ASSET_DOCUMENTATION) {
      validateInputForAssetDocumentationScenario(context, input);
    }
  }

  private void validateFileName(final String fileName) {
    String fileExtension = "";
    int i = fileName.lastIndexOf('.');
    if (i > 0) {
      fileExtension = fileName.substring(i + 1);
    }

    if (!ALLOWED_FILE_EXTENSIONS.contains(fileExtension.toLowerCase())) {
      throw new IllegalArgumentException(
          String.format("Unsupported file extension: %s", fileExtension));
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

  private String generateNewFileId(final GetPresignedUploadUrlInput input) {
    return String.format("%s-%s", UUID.randomUUID().toString(), input.getFileName());
  }

  private String getS3Key(
      final GetPresignedUploadUrlInput input, final String fileId, final String bucketName) {
    UploadDownloadScenario scenario = input.getScenario();

    if (scenario == UploadDownloadScenario.ASSET_DOCUMENTATION) {
      return String.format("%s/product-assets/%s", bucketName, fileId);
    } else {
      throw new IllegalArgumentException("Unsupported upload scenario: " + scenario);
    }
  }
}
