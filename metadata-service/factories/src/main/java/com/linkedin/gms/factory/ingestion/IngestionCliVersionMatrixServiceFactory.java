package com.linkedin.gms.factory.ingestion;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.objectstorage.ObjectStorageClientFactory;
import com.linkedin.metadata.config.CliVersionMatrixConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.HttpMatrixDocumentReader;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.PollingIngestionCliVersionMatrixSource;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageLocation;
import com.linkedin.metadata.version.GitVersion;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires up the per-connector ingestion CLI version matrix.
 *
 * <p>The wiring is split into two beans so that storage and consumption are decoupled:
 *
 * <ul>
 *   <li>{@code ingestionCliVersionMatrixSource} — implements {@link
 *       IngestionCliVersionMatrixSource}. Every backend polls through the same {@link
 *       PollingIngestionCliVersionMatrixSource}; the scheme of {@code
 *       ingestion.cliVersionMatrix.uri} selects only which reader it is given — {@link
 *       HttpMatrixDocumentReader} for {@code http(s)://}, {@link ObjectStorageMatrixDocumentReader}
 *       over an {@link ObjectStorageClient} for {@code s3://}, {@code gs://} and {@code file://}.
 *       An empty URI — or any URI that cannot be served, e.g. an unsupported scheme, a missing
 *       object key, or an unavailable storage client — wires {@link
 *       NoOpIngestionCliVersionMatrixSource} so that connectors fall back to the application
 *       default and GMS startup is never blocked.
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

  // The single place that maps a location onto a storage client, so the matrix inherits the same
  // provider routing and credential resolution (role assumption / endpoint / region) as every other
  // object-storage caller in GMS.
  //
  // Optional: this factory is also loaded by contexts that never import the object-storage factory
  // (mae-consumer reaches it via IngestionSchedulerFactory), where a required injection would fail
  // the whole context at startup. Absent — or present but unable to build a client for the provider
  // — degrades the URI to a no-op matrix source, as every other unusable configuration here does.
  @Autowired(required = false)
  private ObjectStorageClientFactory objectStorageClientFactory;

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

    if (hasPrefix(uri, HTTPS_PREFIX) || hasPrefix(uri, HTTP_PREFIX)) {
      if (hasPrefix(uri, HTTP_PREFIX) && !isEmpty(matrixConfig.getAuthToken())) {
        log.warn(
            "ingestion.cliVersionMatrix.authToken is set on a plain-http URI, so the Authorization "
                + "header will be sent in cleartext. Use https:// unless this endpoint is in-cluster.");
      }
      return new PollingIngestionCliVersionMatrixSource(
          new HttpMatrixDocumentReader(uri, matrixConfig.getAuthToken()), refreshSeconds);
    }
    if (!isEmpty(matrixConfig.getAuthToken())) {
      // Only the HTTP reader sends an Authorization header. Silently dropping the token would leave
      // an operator who set one debugging a 403 from the wrong end — the fix is bucket policy or
      // ambient credentials, not the token they just configured.
      log.warn(
          "ingestion.cliVersionMatrix.authToken is set but is ignored for {}: s3:// and gs:// "
              + "authenticate with GMS's ambient cloud credentials and file:// needs none. Grant the "
              + "GMS identity read access instead (IAM/bucket policy).",
          uri);
    }
    return objectStorageMatrixSource(uri, refreshSeconds);
  }

  /**
   * Builds a matrix source over {@code s3://}, {@code gs://} or {@code file://}. The URI names a
   * single document, so it is split into the root its client is built from and the key within that
   * root; both the split and the provider routing are shared rather than reimplemented here. Any
   * parse failure, unsupported scheme, missing object key, or unbuildable client degrades to a
   * no-op source.
   */
  @Nonnull
  private IngestionCliVersionMatrixSource objectStorageMatrixSource(
      @Nonnull final String uri, final int refreshSeconds) {
    if (objectStorageClientFactory == null) {
      log.warn(
          "ingestion.cliVersionMatrix.uri is {} but this context has no ObjectStorageClientFactory; "
              + "matrix lookups disabled.",
          uri);
      return new NoOpIngestionCliVersionMatrixSource();
    }
    try {
      final ObjectStorageLocation.Document document = ObjectStorageLocation.parseDocument(uri);
      final ObjectStorageClient client = objectStorageClientFactory.clientFor(document.root());
      if (client == null) {
        log.warn(
            "ingestion.cliVersionMatrix.uri is {} but no storage client could be built for it "
                + "(for s3://, set AWS_REGION, AWS_ENDPOINT_URL or datahub.objectStorage.roleArn); "
                + "matrix lookups disabled.",
            uri);
        return new NoOpIngestionCliVersionMatrixSource();
      }
      return new PollingIngestionCliVersionMatrixSource(
          new ObjectStorageMatrixDocumentReader(client, document.objectKey(), uri), refreshSeconds);
    } catch (Exception e) {
      // Covers an unsupported scheme and a URI naming no object (IllegalArgumentException from
      // parseDocument), plus any failure to resolve cloud credentials. Never fatal: connectors use
      // the application default.
      log.warn(
          "Cannot read the ingestion version matrix from {}; matrix lookups disabled. Supported "
              + "URIs are s3://bucket/key, gs://bucket/key, file:///path and http(s)://host/path.",
          uri,
          e);
      return new NoOpIngestionCliVersionMatrixSource();
    }
  }

  private static boolean isEmpty(String s) {
    return s == null || s.trim().isEmpty();
  }

  /**
   * Scheme-prefix match, case-insensitive: URI schemes are case-insensitive per RFC 3986 §3.1, so
   * {@code HTTPS://host/matrix.json} must route to the HTTP reader rather than falling through to
   * object-storage parsing and disabling the matrix. Only the prefix is folded — the rest of an
   * http(s) URL can be case-sensitive (path, query), as can an S3 key or GCS object name.
   */
  private static boolean hasPrefix(@Nonnull String uri, @Nonnull String prefix) {
    return uri.length() >= prefix.length()
        && uri.regionMatches(true, 0, prefix, 0, prefix.length());
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
