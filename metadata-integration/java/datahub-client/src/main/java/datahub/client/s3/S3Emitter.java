package datahub.client.s3;

import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.Callback;
import datahub.client.Emitter;
import datahub.client.MetadataWriteResponse;
import datahub.client.file.FileEmitter;
import datahub.client.file.FileEmitterConfig;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.event.UpsertAspectRequest;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@Slf4j
public class S3Emitter implements Emitter {
  private final Path temporaryFile;
  private final FileEmitter fileEmitter;
  private final S3Client client;
  private final S3EmitterConfig config;

  /**
   * The default constructor
   *
   * @param config
   */
  public S3Emitter(S3EmitterConfig config) throws IOException {
    temporaryFile = Files.createTempFile("datahub_ingest_", "_mcps.json");
    log.info("Emitter created temporary file: {}", this.temporaryFile.toFile());
    FileEmitterConfig fileEmitterConfig =
        FileEmitterConfig.builder()
            .fileName(temporaryFile.toString())
            .eventFormatter(config.getEventFormatter())
            .build();
    fileEmitter = new FileEmitter(fileEmitterConfig);
    S3ClientBuilder s3ClientBuilder = S3Client.builder();

    if (config.getRegion() != null) {
      s3ClientBuilder.region(Region.of(config.getRegion()));
    }

    if (config.getEndpoint() != null) {
      try {
        s3ClientBuilder.endpointOverride(new URI(config.getEndpoint()));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    if (config.getAccessKey() != null && config.getSecretKey() != null) {
      s3ClientBuilder.credentialsProvider(
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey())));
    } else {
      DefaultCredentialsProvider.Builder credentialsProviderBuilder =
          DefaultCredentialsProvider.builder();
      if (config.getProfileName() != null) {
        credentialsProviderBuilder.profileName(config.getProfileName());
      }

      if (config.getProfileFile() != null) {
        credentialsProviderBuilder.profileFile(
            ProfileFile.builder().content(Paths.get(config.getProfileFile())).build());
      }
      s3ClientBuilder.credentialsProvider(credentialsProviderBuilder.build());
    }

    this.client = s3ClientBuilder.build();
    this.config = config;
  }

  private void deleteTemporaryFile() {
    try {
      Files.delete(temporaryFile);
      log.debug("Emitter deleted temporary file: {}", this.temporaryFile.toFile());
    } catch (IOException e) {
      log.warn("Failed to delete temporary file {}", temporaryFile);
    }
  }

  @Override
  public void close() throws IOException {
    log.debug("Closing file {}", this.temporaryFile.toFile());
    fileEmitter.close();
    String key = this.temporaryFile.getFileName().toString();

    // If the target filename is set, use that as the key
    if (config.getFileName() != null) {
      key = config.getFileName();
    }
    if (config.getPathPrefix().endsWith("/")) {
      key = config.getPathPrefix() + key;
    } else {
      key = config.getPathPrefix() + "/" + key;
    }

    if (key.startsWith("/")) {
      key = key.substring(1);
    }

    PutObjectRequest objectRequest =
        PutObjectRequest.builder().bucket(config.getBucketName()).key(key).build();

    log.info(
        "Uploading file {} to S3 with bucket {} and key: {}",
        this.temporaryFile,
        config.getBucketName(),
        key);

    PutObjectResponse response = client.putObject(objectRequest, this.temporaryFile);
    deleteTemporaryFile();
    if (!response.sdkHttpResponse().isSuccessful()) {
      log.error("Failed to upload file to S3. Response: {}", response);
      throw new IOException("Failed to upload file to S3. Response: " + response);
    }
  }

  @Override
  public Future<MetadataWriteResponse> emit(
      @SuppressWarnings("rawtypes") MetadataChangeProposalWrapper mcpw, Callback callback)
      throws IOException {
    return fileEmitter.emit(mcpw, callback);
  }

  @Override
  public Future<MetadataWriteResponse> emit(MetadataChangeProposal mcp, Callback callback)
      throws IOException {
    return fileEmitter.emit(mcp, callback);
  }

  @Override
  public boolean testConnection() throws IOException, ExecutionException, InterruptedException {
    throw new UnsupportedOperationException("testConnection not relevant for File Emitter");
  }

  @Override
  public Future<MetadataWriteResponse> emit(List<UpsertAspectRequest> request, Callback callback)
      throws IOException {
    throw new UnsupportedOperationException("UpsertAspectRequest not relevant for File Emitter");
  }

  private Future<MetadataWriteResponse> createFailureFuture(String message) {
    return new Future<MetadataWriteResponse>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public MetadataWriteResponse get() throws InterruptedException, ExecutionException {
        return MetadataWriteResponse.builder().success(false).responseContent(message).build();
      }

      @Override
      public MetadataWriteResponse get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return this.get();
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }
    };
  }
}
