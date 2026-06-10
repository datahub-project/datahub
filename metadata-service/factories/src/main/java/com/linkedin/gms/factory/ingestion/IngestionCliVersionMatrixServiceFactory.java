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

/**
 * Wires up the per-connector ingestion CLI version matrix.
 *
 * <p>The wiring is split into two beans so that storage and consumption are decoupled:
 *
 * <ul>
 *   <li>{@code ingestionCliVersionMatrixSource} — implements {@link
 *       IngestionCliVersionMatrixSource}. The implementation is selected from {@code
 *       ingestion.cliVersionMatrix.source}: {@code "none"} short-circuits to {@link
 *       NoOpIngestionCliVersionMatrixSource}; otherwise an HTTP source is wired when {@code
 *       http.url} is set and a no-op source is wired when the URL is empty. Future backends (GMS
 *       aspect, config server, …) add their own discriminator value handled here.
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

  private static final String SOURCE_NONE = "none";

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configProvider;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  /**
   * Picks the storage backend for the matrix from {@code ingestion.cliVersionMatrix.source}.
   * Explicit {@code "none"} is a kill-switch that always wins. Otherwise the HTTP source is wired
   * when {@code http.url} is non-empty, and a no-op source when the URL is empty.
   */
  @Bean(name = "ingestionCliVersionMatrixSource")
  @Nonnull
  protected IngestionCliVersionMatrixSource ingestionCliVersionMatrixSource() {
    CliVersionMatrixConfiguration matrixConfig =
        configProvider.getIngestion().getCliVersionMatrix();
    if (matrixConfig == null) {
      return new NoOpIngestionCliVersionMatrixSource();
    }
    if (SOURCE_NONE.equalsIgnoreCase(trim(matrixConfig.getSource()))) {
      return new NoOpIngestionCliVersionMatrixSource();
    }
    HttpMatrixSourceConfiguration httpConfig = matrixConfig.getHttp();
    if (httpConfig == null || httpConfig.getUrl() == null || httpConfig.getUrl().isEmpty()) {
      return new NoOpIngestionCliVersionMatrixSource();
    }
    return new HttpUrlIngestionCliVersionMatrixSource(
        httpConfig.getUrl(), httpConfig.getRefreshSeconds(), httpConfig.getAuthToken());
  }

  private static String trim(String s) {
    return s == null ? null : s.trim();
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
