package com.linkedin.gms.factory.ingestion;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CliVersionMatrixConfiguration;
import com.linkedin.metadata.config.GcsMatrixSourceConfiguration;
import com.linkedin.metadata.config.HttpMatrixSourceConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.config.S3MatrixSourceConfiguration;
import com.linkedin.metadata.ingestion.HttpUrlIngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource;
import com.linkedin.metadata.utils.aws.S3Util;
import com.linkedin.metadata.utils.gcs.GcsUtil;
import com.linkedin.metadata.version.GitVersion;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
 *       IngestionCliVersionMatrixSource}. The implementation is selected from {@code
 *       ingestion.cliVersionMatrix.source}: {@code "none"} short-circuits to {@link
 *       NoOpIngestionCliVersionMatrixSource}; {@code "s3"} wires an {@link
 *       S3IngestionCliVersionMatrixSource} and {@code "gcs"} wires a {@link
 *       GcsIngestionCliVersionMatrixSource} when usable (both degrade to no-op if config is
 *       incomplete or no client is available, never a startup failure); otherwise {@code "http"}
 *       (the default) wires an HTTP source when {@code http.url} is set and a no-op when the URL is
 *       empty. Future backends (GMS aspect, config server, …) add their own discriminator value
 *       handled here.
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

  private static final String SOURCE_NONE = "none";
  private static final String SOURCE_S3 = "s3";
  private static final String SOURCE_GCS = "gcs";

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configProvider;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  // Optional: present only when AWS is configured (AWS_REGION / datahub.s3.roleArn); null
  // otherwise, in which case source=s3 degrades to a no-op matrix source with a warning rather
  // than failing.
  @Autowired(required = false)
  @Qualifier("s3Util")
  private S3Util s3Util;

  // Optional: present only when ambient GCP credentials are available; null otherwise, in which
  // case source=gcs degrades to a no-op matrix source with a warning rather than failing.
  @Autowired(required = false)
  @Qualifier("gcsUtil")
  private GcsUtil gcsUtil;

  /**
   * Picks the storage backend for the matrix from {@code ingestion.cliVersionMatrix.source}.
   * Explicit {@code "none"} is a kill-switch that always wins. {@code "s3"}/{@code "gcs"} select
   * the corresponding cloud source (degrades to no-op when config is incomplete or no client is
   * available). Otherwise the HTTP source is wired when {@code http.url} is non-empty, and a no-op
   * source when the URL is empty.
   */
  @Bean(name = "ingestionCliVersionMatrixSource")
  @Nonnull
  protected IngestionCliVersionMatrixSource ingestionCliVersionMatrixSource() {
    CliVersionMatrixConfiguration matrixConfig =
        configProvider.getIngestion().getCliVersionMatrix();
    if (matrixConfig == null) {
      return new NoOpIngestionCliVersionMatrixSource();
    }
    String source = trim(matrixConfig.getSource());
    if (SOURCE_NONE.equalsIgnoreCase(source)) {
      return new NoOpIngestionCliVersionMatrixSource();
    }
    if (SOURCE_S3.equalsIgnoreCase(source)) {
      IngestionCliVersionMatrixSource s3 = s3MatrixSourceOrNull(matrixConfig.getS3());
      return s3 != null ? s3 : new NoOpIngestionCliVersionMatrixSource();
    }
    if (SOURCE_GCS.equalsIgnoreCase(source)) {
      IngestionCliVersionMatrixSource gcs = gcsMatrixSourceOrNull(matrixConfig.getGcs());
      return gcs != null ? gcs : new NoOpIngestionCliVersionMatrixSource();
    }
    // Default: HTTP source (covers explicit "http" and any unrecognised value).
    HttpMatrixSourceConfiguration httpConfig = matrixConfig.getHttp();
    if (httpConfig == null || httpConfig.getUrl() == null || httpConfig.getUrl().isEmpty()) {
      return new NoOpIngestionCliVersionMatrixSource();
    }
    return new HttpUrlIngestionCliVersionMatrixSource(
        httpConfig.getUrl(), httpConfig.getRefreshSeconds(), httpConfig.getAuthToken());
  }

  /**
   * Builds the S3-backed matrix source, or {@code null} when the S3 config is incomplete or no S3
   * client bean is available — so a misconfiguration degrades to a no-op (the application default)
   * rather than failing GMS startup.
   */
  @Nullable
  private IngestionCliVersionMatrixSource s3MatrixSourceOrNull(
      @Nullable final S3MatrixSourceConfiguration s3Config) {
    if (s3Config == null || isEmpty(s3Config.getBucket()) || isEmpty(s3Config.getKey())) {
      log.warn("ingestion.cliVersionMatrix.source=s3 but bucket/key are not set.");
      return null;
    }
    // A non-positive refresh interval would make scheduleAtFixedRate throw in the source
    // constructor and fail GMS startup; degrade to the no-op (application default) instead.
    if (s3Config.getRefreshSeconds() <= 0) {
      log.warn(
          "ingestion.cliVersionMatrix.source=s3 but refreshSeconds={} is not positive.",
          s3Config.getRefreshSeconds());
      return null;
    }
    if (s3Util == null) {
      log.warn(
          "ingestion.cliVersionMatrix.source=s3 but no s3Util bean is available "
              + "(set AWS_REGION or datahub.s3.roleArn).");
      return null;
    }
    return new S3IngestionCliVersionMatrixSource(
        s3Util, s3Config.getBucket(), s3Config.getKey(), s3Config.getRefreshSeconds());
  }

  /**
   * Builds the GCS-backed matrix source, or {@code null} when the GCS config is incomplete or no
   * GCS client bean is available — so a misconfiguration degrades to a no-op (the application
   * default) rather than failing GMS startup.
   */
  @Nullable
  private IngestionCliVersionMatrixSource gcsMatrixSourceOrNull(
      @Nullable final GcsMatrixSourceConfiguration gcsConfig) {
    if (gcsConfig == null || isEmpty(gcsConfig.getBucket()) || isEmpty(gcsConfig.getKey())) {
      log.warn("ingestion.cliVersionMatrix.source=gcs but bucket/key are not set.");
      return null;
    }
    // A non-positive refresh interval would make scheduleAtFixedRate throw in the source
    // constructor and fail GMS startup; degrade to the no-op (application default) instead.
    if (gcsConfig.getRefreshSeconds() <= 0) {
      log.warn(
          "ingestion.cliVersionMatrix.source=gcs but refreshSeconds={} is not positive.",
          gcsConfig.getRefreshSeconds());
      return null;
    }
    if (gcsUtil == null) {
      log.warn(
          "ingestion.cliVersionMatrix.source=gcs but no gcsUtil bean is available "
              + "(no ambient GCP credentials found).");
      return null;
    }
    return new GcsIngestionCliVersionMatrixSource(
        gcsUtil, gcsConfig.getBucket(), gcsConfig.getKey(), gcsConfig.getRefreshSeconds());
  }

  private static String trim(String s) {
    return s == null ? null : s.trim();
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
