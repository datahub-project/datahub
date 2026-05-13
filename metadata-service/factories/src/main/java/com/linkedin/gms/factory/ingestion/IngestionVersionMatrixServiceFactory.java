package com.linkedin.gms.factory.ingestion;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.HttpUrlMatrixSource;
import com.linkedin.metadata.ingestion.IngestionVersionMatrixService;
import com.linkedin.metadata.ingestion.MatrixSource;
import com.linkedin.metadata.ingestion.NoOpMatrixSource;
import com.linkedin.metadata.version.GitVersion;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Wires up the per-connector CLI version matrix.
 *
 * <p>The wiring is split into two beans so that storage and consumption are decoupled:
 *
 * <ul>
 *   <li>{@code matrixSource} — implements {@link MatrixSource}; chosen based on configuration.
 *       Today the only "live" implementation is {@link HttpUrlMatrixSource}, picked when {@code
 *       ingestion.versionMatrixUrl} is set. Otherwise a {@link NoOpMatrixSource} is bound. Future
 *       implementations (GMS-aspect-backed, config-server-backed, …) can plug in here without any
 *       change to the consumer service.
 *   <li>{@code ingestionVersionMatrixService} — consumes whichever {@link MatrixSource} is bound
 *       and applies the resolution policy (cohort → connector default → workspace default).
 * </ul>
 *
 * <p>The deployment identifier is sourced from {@code ingestion.deploymentId}, which is bound to
 * the {@code DATAHUB_EXECUTOR_CUSTOMER_ID} env var injected by the Acryl Cloud Helm chart from the
 * K8s namespace. In single-tenant / OSS deployments it is typically unset, so the deployment id is
 * empty and cohort matching never fires — only the connector-level {@code _default} from the matrix
 * applies.
 */
@Configuration
public class IngestionVersionMatrixServiceFactory {

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configProvider;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  /**
   * Picks the storage backend for the matrix. Today this is either HTTP-fetch or no-op; the
   * decision is driven by whether a URL is configured. New backends should be added here behind an
   * explicit config flag rather than by replacing the existing decision.
   */
  @Bean(name = "matrixSource")
  @Scope("singleton")
  @Nonnull
  protected MatrixSource matrixSource() {
    IngestionConfiguration ingestionConfig = configProvider.getIngestion();
    String url = ingestionConfig.getVersionMatrixUrl();
    if (url == null || url.isEmpty()) {
      return new NoOpMatrixSource();
    }
    return new HttpUrlMatrixSource(
        url,
        ingestionConfig.getVersionMatrixRefreshSeconds(),
        ingestionConfig.getVersionMatrixAuthToken());
  }

  @Bean(name = "ingestionVersionMatrixService")
  @Scope("singleton")
  @Nonnull
  protected IngestionVersionMatrixService getInstance(
      @Qualifier("matrixSource") final MatrixSource matrixSource) {
    IngestionConfiguration ingestionConfig = configProvider.getIngestion();
    String serverVersion = (String) gitVersion.toConfig().get("version");
    return new IngestionVersionMatrixService(
        matrixSource, serverVersion, ingestionConfig.getDeploymentId());
  }
}
