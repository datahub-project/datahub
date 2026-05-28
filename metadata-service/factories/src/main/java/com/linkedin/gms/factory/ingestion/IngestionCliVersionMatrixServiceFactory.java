package com.linkedin.gms.factory.ingestion;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.HttpUrlIngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource;
import com.linkedin.metadata.version.GitVersion;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Wires up the per-connector ingestion CLI version matrix.
 *
 * <p>The wiring is split into two beans so that storage and consumption are decoupled:
 *
 * <ul>
 *   <li>{@code ingestionCliVersionMatrixSource} — implements {@link
 *       IngestionCliVersionMatrixSource}; chosen based on configuration. Today the only "live"
 *       implementation is {@link HttpUrlIngestionCliVersionMatrixSource}, picked when {@code
 *       ingestion.versionMatrixUrl} is set. Otherwise a {@link NoOpIngestionCliVersionMatrixSource}
 *       is bound. Future implementations (GMS-aspect-backed, config-server-backed, …) can plug in
 *       here without any change to the consumer service.
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
@Configuration
public class IngestionCliVersionMatrixServiceFactory {

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
  @Bean(name = "ingestionCliVersionMatrixSource")
  @Scope("singleton")
  @Nonnull
  protected IngestionCliVersionMatrixSource ingestionCliVersionMatrixSource() {
    IngestionConfiguration ingestionConfig = configProvider.getIngestion();
    String url = ingestionConfig.getVersionMatrixUrl();
    if (url == null || url.isEmpty()) {
      return new NoOpIngestionCliVersionMatrixSource();
    }
    return new HttpUrlIngestionCliVersionMatrixSource(
        url,
        ingestionConfig.getVersionMatrixRefreshSeconds(),
        ingestionConfig.getVersionMatrixAuthToken());
  }

  @Bean(name = "ingestionCliVersionMatrixService")
  @Scope("singleton")
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
