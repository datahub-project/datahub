package com.linkedin.gms.factory.ingestion;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CliVersionMatrixConfiguration;
import com.linkedin.metadata.config.HttpMatrixSourceConfiguration;
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
 *       IngestionCliVersionMatrixSource}. The implementation is selected from {@code
 *       ingestion.cliVersionMatrix.source}: {@code "http"} → {@link
 *       HttpUrlIngestionCliVersionMatrixSource}, {@code "none"} → {@link
 *       NoOpIngestionCliVersionMatrixSource}. When the discriminator is unset, the choice is
 *       inferred from {@code ingestion.cliVersionMatrix.http.url} so existing deployments keep
 *       working without an env change. Future implementations (GMS-aspect-backed,
 *       config-server-backed, …) get a new discriminator value here.
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

  private static final String SOURCE_HTTP = "http";
  private static final String SOURCE_NONE = "none";

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configProvider;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  /**
   * Picks the storage backend for the matrix. Reads {@code ingestion.cliVersionMatrix.source} as
   * the primary signal; falls back to inferring from {@code http.url} presence when the
   * discriminator is unset (backward-compat for deployments that pre-date this discriminator).
   */
  @Bean(name = "ingestionCliVersionMatrixSource")
  @Scope("singleton")
  @Nonnull
  protected IngestionCliVersionMatrixSource ingestionCliVersionMatrixSource() {
    CliVersionMatrixConfiguration matrixConfig =
        configProvider.getIngestion().getCliVersionMatrix();
    if (matrixConfig == null) {
      return new NoOpIngestionCliVersionMatrixSource();
    }

    HttpMatrixSourceConfiguration httpConfig = matrixConfig.getHttp();
    boolean httpUrlPresent =
        httpConfig != null && httpConfig.getUrl() != null && !httpConfig.getUrl().isEmpty();

    if (resolveSource(matrixConfig.getSource(), httpUrlPresent).equals(SOURCE_HTTP)) {
      return new HttpUrlIngestionCliVersionMatrixSource(
          httpConfig.getUrl(), httpConfig.getRefreshSeconds(), httpConfig.getAuthToken());
    }
    return new NoOpIngestionCliVersionMatrixSource();
  }

  /**
   * Resolve the active source backend. Explicit {@code source} wins; an unset value is inferred
   * from URL presence so deployments that pre-date this discriminator continue to work.
   */
  private static String resolveSource(String configuredSource, boolean httpUrlPresent) {
    if (configuredSource != null && !configuredSource.trim().isEmpty()) {
      return configuredSource.trim().toLowerCase();
    }
    return httpUrlPresent ? SOURCE_HTTP : SOURCE_NONE;
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
