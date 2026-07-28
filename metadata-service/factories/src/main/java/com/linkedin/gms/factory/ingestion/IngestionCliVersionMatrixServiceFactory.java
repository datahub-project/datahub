package com.linkedin.gms.factory.ingestion;

import com.google.cloud.storage.StorageOptions;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.objectstorage.ObjectStorageClientFactory;
import com.linkedin.metadata.config.CliVersionMatrixConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.HttpUrlIngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource;
import com.linkedin.metadata.utils.objectstorage.GcsObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.LocalObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageLocation;
import com.linkedin.metadata.utils.objectstorage.S3ObjectStorageClient;
import com.linkedin.metadata.version.GitVersion;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Wires up the per-connector ingestion CLI version matrix.
 *
 * <p>The wiring is split into two beans so that storage and consumption are decoupled:
 *
 * <ul>
 *   <li>{@code ingestionCliVersionMatrixSource} — implements {@link
 *       IngestionCliVersionMatrixSource}. The backend is selected by the scheme of {@code
 *       ingestion.cliVersionMatrix.uri}: {@code http(s)://} fetches over HTTP, while {@code s3://},
 *       {@code gs://} and {@code file://} are read through an {@link ObjectStorageClient}. An empty
 *       URI — or any URI that cannot be served, e.g. an unsupported scheme, a missing object key,
 *       or an unavailable storage client — wires {@link NoOpIngestionCliVersionMatrixSource} so
 *       that connectors fall back to the application default and GMS startup is never blocked.
 *   <li>{@code ingestionCliVersionMatrixService} — consumes whichever {@link
 *       IngestionCliVersionMatrixSource} is bound and applies the resolution policy (cohort →
 *       connector default → application default).
 * </ul>
 *
 * <p>The deployment identifier is sourced from {@code ingestion.deploymentId}, which is bound to
 * the {@code DATAHUB_EXECUTOR_CUSTOMER_ID} env var injected by the Acryl Cloud Helm chart from the
 * K8s namespace. In single-tenant / OSS deployments it is typically unset, so the deployment id is
 * empty and cohort matching never fires — only the connector-level {@code _default} from the matrix
 * applies.
 */
@Slf4j
@Configuration
public class IngestionCliVersionMatrixServiceFactory {

  private static final String HTTP_PREFIX = "http://";
  private static final String HTTPS_PREFIX = "https://";

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configProvider;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  // Borrowed rather than building a second S3 client so the matrix inherits the same role
  // assumption / endpoint / region resolution as every other S3 caller in GMS. Its
  // createS3Client() yields null when AWS is not configured, in which case an s3:// URI degrades
  // to a no-op matrix source with a warning rather than failing startup.
  @Autowired private ObjectStorageClientFactory objectStorageClientFactory;

  /**
   * Picks the matrix backend from the scheme of {@code ingestion.cliVersionMatrix.uri}. Every
   * unusable configuration degrades to {@link NoOpIngestionCliVersionMatrixSource} (connectors use
   * the application default) instead of failing GMS startup.
   */
  @Bean(name = "ingestionCliVersionMatrixSource")
  @Nonnull
  protected IngestionCliVersionMatrixSource ingestionCliVersionMatrixSource() {
    CliVersionMatrixConfiguration matrixConfig =
        configProvider.getIngestion().getCliVersionMatrix();
    if (matrixConfig == null || isEmpty(matrixConfig.getUri())) {
      return new NoOpIngestionCliVersionMatrixSource();
    }
    final String uri = matrixConfig.getUri().trim();
    final int refreshSeconds = matrixConfig.getRefreshSeconds();
    // A non-positive interval would make scheduleAtFixedRate throw in the source constructor and
    // fail GMS startup; degrade to the application default instead.
    if (refreshSeconds <= 0) {
      log.warn(
          "ingestion.cliVersionMatrix.refreshSeconds={} is not positive; matrix lookups disabled.",
          refreshSeconds);
      return new NoOpIngestionCliVersionMatrixSource();
    }

    if (uri.startsWith(HTTPS_PREFIX) || uri.startsWith(HTTP_PREFIX)) {
      if (uri.startsWith(HTTP_PREFIX) && !isEmpty(matrixConfig.getAuthToken())) {
        log.warn(
            "ingestion.cliVersionMatrix.authToken is set on a plain-http URI, so the Authorization "
                + "header will be sent in cleartext. Use https:// unless this endpoint is in-cluster.");
      }
      return new HttpUrlIngestionCliVersionMatrixSource(
          uri, refreshSeconds, matrixConfig.getAuthToken());
    }
    return objectStorageMatrixSource(uri, refreshSeconds);
  }

  /**
   * Builds a matrix source over {@code s3://}, {@code gs://} or {@code file://} using the shared
   * {@link ObjectStorageLocation} URI parsing. Any parse failure, unsupported scheme, or missing
   * client degrades to a no-op source.
   */
  @Nonnull
  private IngestionCliVersionMatrixSource objectStorageMatrixSource(
      @Nonnull final String uri, final int refreshSeconds) {
    try {
      final ObjectStorageLocation location = ObjectStorageLocation.parse(uri);
      return switch (location.provider()) {
        case S3 -> {
          S3Client s3Client = objectStorageClientFactory.createS3Client();
          if (s3Client == null) {
            log.warn(
                "ingestion.cliVersionMatrix.uri is {} but no S3 client could be built "
                    + "(set AWS_REGION, AWS_ENDPOINT_URL or datahub.objectStorage.roleArn); "
                    + "matrix lookups disabled.",
                uri);
            yield new NoOpIngestionCliVersionMatrixSource();
          }
          yield matrixSource(
              new S3ObjectStorageClient(s3Client, location.bucket(), null),
              location.keyPrefix(),
              uri,
              refreshSeconds);
        }
        case GCS -> matrixSource(
            new GcsObjectStorageClient(
                StorageOptions.getDefaultInstance().getService(), location.bucket(), null),
            location.keyPrefix(),
            uri,
            refreshSeconds);
        case LOCAL -> localMatrixSource(location, uri, refreshSeconds);
      };
    } catch (Exception e) {
      // Covers an unsupported scheme (IllegalArgumentException from parse) and any failure to
      // resolve cloud credentials. Never fatal: connectors use the application default.
      log.warn(
          "Cannot read the ingestion version matrix from {}; matrix lookups disabled. Supported "
              + "URIs are s3://bucket/key, gs://bucket/key, file:///path and http(s)://host/path.",
          uri,
          e);
      return new NoOpIngestionCliVersionMatrixSource();
    }
  }

  /**
   * A {@code file://} URI addresses the matrix document itself, whereas {@link
   * LocalObjectStorageClient} is rooted at a directory — so the parent directory becomes the root
   * and the file name becomes the object key.
   */
  @Nonnull
  private IngestionCliVersionMatrixSource localMatrixSource(
      @Nonnull final ObjectStorageLocation location,
      @Nonnull final String uri,
      final int refreshSeconds) {
    Path path = Path.of(location.localRoot());
    Path parent = path.getParent();
    if (parent == null) {
      log.warn("ingestion.cliVersionMatrix.uri {} must point at a file; matrix disabled.", uri);
      return new NoOpIngestionCliVersionMatrixSource();
    }
    return matrixSource(
        new LocalObjectStorageClient(parent.toString()),
        path.getFileName().toString(),
        uri,
        refreshSeconds);
  }

  /**
   * Final assembly step shared by every provider. {@code objectKey} is everything after the bucket
   * in the URI, so a bucket-only URI is a misconfiguration rather than a readable location.
   */
  @Nonnull
  private IngestionCliVersionMatrixSource matrixSource(
      @Nonnull final ObjectStorageClient client,
      final String objectKey,
      @Nonnull final String uri,
      final int refreshSeconds) {
    if (isEmpty(objectKey)) {
      log.warn(
          "ingestion.cliVersionMatrix.uri {} does not include an object key "
              + "(expected e.g. s3://bucket/matrix.json); matrix lookups disabled.",
          uri);
      return new NoOpIngestionCliVersionMatrixSource();
    }
    return new ObjectStorageIngestionCliVersionMatrixSource(client, objectKey, uri, refreshSeconds);
  }

  private static boolean isEmpty(String s) {
    return s == null || s.trim().isEmpty();
  }

  @Bean(name = "ingestionCliVersionMatrixService")
  @Nonnull
  protected IngestionCliVersionMatrixService getInstance(
      @Qualifier("ingestionCliVersionMatrixSource")
          final IngestionCliVersionMatrixSource matrixSource) {
    IngestionConfiguration ingestionConfig = configProvider.getIngestion();
    String serverVersion = (String) gitVersion.toConfig().get("version");
    return new IngestionCliVersionMatrixService(
        matrixSource, serverVersion, ingestionConfig.getDeploymentId());
  }
}
