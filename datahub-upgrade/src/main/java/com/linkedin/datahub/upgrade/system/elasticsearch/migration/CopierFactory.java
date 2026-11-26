package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.CopyMode;
import com.linkedin.metadata.config.search.CopySettings;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.config.search.SearchClusterUri;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/** Builds the copier a migration's {@code copy.mode} asks for. */
public final class CopierFactory {
  private CopierFactory() {}

  @Nonnull
  public static CrossClusterCopier create(
      @Nonnull final OperationContext opContext,
      @Nonnull final CopyMode mode,
      @Nonnull final CopySettings settings,
      @Nonnull final SearchClusterRegistry.ClusterConnection source,
      @Nonnull final SearchClusterRegistry.ClusterConnection target,
      @Nonnull final String upgradeId) {
    final ClusterHttp sourceHttp = new ClusterHttp(opContext, source.getClient());
    final ClusterHttp targetHttp = new ClusterHttp(opContext, target.getClient());
    switch (mode) {
      case IN_PROCESS:
        return new InProcessCopier(
            sourceHttp,
            targetHttp,
            settings.getBatchSize(),
            settings.getCursorKeepAlive() == null ? "5m" : settings.getCursorKeepAlive(),
            settings.getCheckpointEveryPages(),
            InProcessCopier.Pacer.NONE);
      case REMOTE_REINDEX:
        {
          final SearchClusterSettings sourceSettings =
              source.getConfig().getCluster(source.getName());
          final SearchClusterUri uri = sourceSettings.parsedUri(source.getName());
          final String host =
              settings.getRemote().getHost() != null
                  ? settings.getRemote().getHost()
                  : (uri.isUseSSL() ? "https" : "http")
                      + "://"
                      + uri.getHost()
                      + ":"
                      + uri.getPort();
          return new RemoteReindexCopier(
              targetHttp,
              RemoteReindexCopier.Source.of(
                  host,
                  sourceSettings.getUsername(),
                  sourceSettings.getPassword(),
                  settings.getRemote()),
              "clustermigration|" + upgradeId,
              settings.getBatchSize(),
              settings.getRequestsPerSecond());
        }
      default:
        throw new IllegalArgumentException("No copier for mode " + mode);
    }
  }
}
