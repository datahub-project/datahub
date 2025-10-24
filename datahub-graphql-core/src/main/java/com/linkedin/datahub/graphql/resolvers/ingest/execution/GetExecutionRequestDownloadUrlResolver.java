package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.datahub.authorization.AuthUtil.isAuthorized;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.GetExecutionRequestDownloadUrlInput;
import com.linkedin.datahub.graphql.generated.GetExecutionRequestDownloadUrlResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestArtifactsLocation;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.utils.aws.S3Util;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class GetExecutionRequestDownloadUrlResolver
    implements DataFetcher<CompletableFuture<GetExecutionRequestDownloadUrlResult>> {

  private static final String EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME =
      "dataHubExecutionRequestArtifactsLocation";
  private static final int DEFAULT_EXPIRATION_SECONDS = 3600; // 1 hour

  private final EntityClient entityClient;
  private final S3Util s3Util;

  @Override
  public CompletableFuture<GetExecutionRequestDownloadUrlResult> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final GetExecutionRequestDownloadUrlInput input =
              bindArgument(
                  environment.getArgument("input"), GetExecutionRequestDownloadUrlInput.class);

          try {
            final Urn executionRequestUrn = validateAndParseUrn(input.getExecutionRequestUrn());
            validateAuthorization(context);
            final String s3Location = fetchUploadLocation(context, executionRequestUrn);
            final S3Location parsedLocation = parseS3Location(s3Location);
            final int expirationSeconds = validateExpirationSeconds(input.getExpirationSeconds());
            final String presignedUrl = generatePresignedUrl(parsedLocation, expirationSeconds);

            return createResult(presignedUrl, expirationSeconds);

          } catch (Exception e) {
            log.error("Failed to generate download URL for execution request", e);
            throw new RuntimeException(
                String.format("Failed to generate download URL: %s", e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Urn validateAndParseUrn(final String executionRequestUrnString) {
    final Urn executionRequestUrn = UrnUtils.getUrn(executionRequestUrnString);

    if (!executionRequestUrn.getEntityType().equals(EXECUTION_REQUEST_ENTITY_NAME)) {
      throw new DataHubGraphQLException(
          "URN must be an execution request URN", DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    return executionRequestUrn;
  }

  private void validateAuthorization(final QueryContext context) {
    if (!isAuthorized(context.getOperationContext(), PoliciesConfig.MANAGE_INGESTION_PRIVILEGE)) {
      throw new AuthorizationException(
          "Unauthorized to download files. Missing MANAGE_INGESTION privilege");
    }
  }

  private String fetchUploadLocation(final QueryContext context, final Urn executionRequestUrn)
      throws RemoteInvocationException, URISyntaxException {
    final Map<Urn, EntityResponse> response =
        entityClient.batchGetV2(
            context.getOperationContext(),
            EXECUTION_REQUEST_ENTITY_NAME,
            ImmutableSet.of(executionRequestUrn),
            ImmutableSet.of(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME));

    if (!response.containsKey(executionRequestUrn)) {
      throw new DataHubGraphQLException(
          String.format(
              "Failed to find execution request with urn %s", executionRequestUrn.toString()),
          DataHubGraphQLErrorCode.NOT_FOUND);
    }

    final EntityResponse entityResponse = response.get(executionRequestUrn);
    if (!entityResponse
        .getAspects()
        .containsKey(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME)) {
      throw new DataHubGraphQLException(
          "No upload location found for this execution request", DataHubGraphQLErrorCode.NOT_FOUND);
    }

    final EnvelopedAspect envelopedAspect =
        entityResponse.getAspects().get(EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME);
    final ExecutionRequestArtifactsLocation uploadLocation =
        new ExecutionRequestArtifactsLocation(envelopedAspect.getValue().data());

    final String s3Location = uploadLocation.getLocation();
    log.info(
        "Retrieved upload location for execution request {}: {}", executionRequestUrn, s3Location);

    return s3Location;
  }

  private S3Location parseS3Location(final String s3Location) {
    final URI s3Uri = URI.create(s3Location);
    if (!"s3".equals(s3Uri.getScheme())) {
      throw new DataHubGraphQLException(
          "Only S3 locations are supported for download URLs", DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    final String bucket = s3Uri.getHost();
    final String key =
        s3Uri.getPath().startsWith("/") ? s3Uri.getPath().substring(1) : s3Uri.getPath();

    if (bucket == null || bucket.isEmpty() || key.isEmpty()) {
      throw new DataHubGraphQLException(
          "Invalid S3 location format. Expected: s3://bucket/key",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    return new S3Location(bucket, key);
  }

  private int validateExpirationSeconds(final Integer inputExpirationSeconds) {
    final int expirationSeconds =
        inputExpirationSeconds != null ? inputExpirationSeconds : DEFAULT_EXPIRATION_SECONDS;

    if (expirationSeconds <= 0 || expirationSeconds > 604800) {
      throw new DataHubGraphQLException(
          "Expiration seconds must be between 1 and 604800 (7 days)",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    return expirationSeconds;
  }

  private String generatePresignedUrl(final S3Location s3Location, final int expirationSeconds) {
    final String presignedUrl =
        s3Util.generatePresignedDownloadUrl(s3Location.bucket, s3Location.key, expirationSeconds);

    log.info(
        "Generated pre-signed URL for bucket: {}, key: {}, expires in: {}s",
        s3Location.bucket,
        s3Location.key,
        expirationSeconds);

    return presignedUrl;
  }

  private GetExecutionRequestDownloadUrlResult createResult(
      final String presignedUrl, final int expirationSeconds) {
    final GetExecutionRequestDownloadUrlResult result = new GetExecutionRequestDownloadUrlResult();
    result.setDownloadUrl(presignedUrl);
    result.setExpiresIn(expirationSeconds);
    return result;
  }

  private static class S3Location {
    final String bucket;
    final String key;

    S3Location(final String bucket, final String key) {
      this.bucket = bucket;
      this.key = key;
    }
  }
}
