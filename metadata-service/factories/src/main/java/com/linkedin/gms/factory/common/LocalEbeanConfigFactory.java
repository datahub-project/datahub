package com.linkedin.gms.factory.common;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.RequestStats;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import io.ebean.datasource.DataSourcePoolListener;
import java.sql.Connection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@Slf4j
@Configuration
public class LocalEbeanConfigFactory {

  @Value("${ebean.username}")
  private String ebeanDatasourceUsername;

  @Value("${ebean.password}")
  private String ebeanDatasourcePassword;

  @Value("${ebean.driver}")
  private String ebeanDatasourceDriver;

  @Value("${ebean.url}")
  private String ebeanDatasourceUrl;

  @Value("${ebean.minConnections:2}")
  private Integer ebeanMinConnections;

  @Value("${ebean.maxConnections:50}")
  private Integer ebeanMaxConnections;

  @Value("${ebean.maxInactiveTimeSeconds:120}")
  private Integer ebeanMaxInactiveTimeSecs;

  @Value("${ebean.maxAgeMinutes:120}")
  private Integer ebeanMaxAgeMinutes;

  @Value("${ebean.leakTimeMinutes:15}")
  private Integer ebeanLeakTimeMinutes;

  @Value("${ebean.waitTimeoutMillis:1000}")
  private Integer ebeanWaitTimeoutMillis;

  @Value("${ebean.autoCommit:true}")
  private Boolean ebeanAutoCommit;

  @Value("${ebean.autoCreateDdl:false}")
  private Boolean ebeanAutoCreate;

  @Value("${ebean.postgresUseIamAuth:false}")
  private Boolean postgresUseIamAuth;

  @Value("${ebean.useIamAuth:false}")
  private Boolean useIamAuth;

  @Value("${ebean.cloudProvider:auto}")
  private String cloudProvider;

  // Environment variable properties for cloud detection
  @Value("${AWS_REGION:#{null}}")
  private String awsRegion;

  @Value("${AWS_ACCESS_KEY_ID:#{null}}")
  private String awsAccessKeyId;

  @Value("${AWS_SECRET_ACCESS_KEY:#{null}}")
  private String awsSecretAccessKey;

  @Value("${AWS_SESSION_TOKEN:#{null}}")
  private String awsSessionToken;

  @Value("${GOOGLE_APPLICATION_CREDENTIALS:#{null}}")
  private String googleApplicationCredentials;

  @Value("${GCP_PROJECT:#{null}}")
  private String gcpProject;

  @Value("${INSTANCE_CONNECTION_NAME:#{null}}")
  private String instanceConnectionName;

  @Autowired(required = false)
  @Qualifier("defaultAwsCredentialsProvider")
  private AwsCredentialsProvider defaultAwsCredentialsProvider;

  @Value("${telemetry.requestAttribution.postgresActorComment:false}")
  private boolean postgresActorComment;

  public static DataSourcePoolListener getListenerToTrackCounts(
      MetricUtils metricUtils, String metricName) {
    final String counterName = "ebeans_connection_pool_size_" + metricName;
    // Request attribution (off by default): time how long each request holds a connection. Borrow
    // and return happen on the same thread, so a ThreadLocal is enough; when no accumulator is in
    // scope this costs one nanoTime call per borrow and nothing else.
    final ThreadLocal<long[]> borrowedAt = ThreadLocal.withInitial(() -> new long[1]);
    return new DataSourcePoolListener() {
      @Override
      public void onAfterBorrowConnection(Connection connection) {
        if (metricUtils != null) metricUtils.increment(counterName, 1);
        borrowedAt.get()[0] = System.nanoTime();
        RequestStats.current().ifPresent(s -> s.recordDbBackendPid(PgBackendPid.of(connection)));
      }

      @Override
      public void onBeforeReturnConnection(Connection connection) {
        if (metricUtils != null) metricUtils.increment(counterName, -1);
        long start = borrowedAt.get()[0];
        if (start != 0L) {
          borrowedAt.get()[0] = 0L;
          RequestStats.current().ifPresent(s -> s.recordDb(System.nanoTime() - start));
        }
      }
    };
  }

  @Bean("ebeanDataSourceConfig")
  public DataSourceConfig buildDataSourceConfig(MetricUtils metricUtils) {
    log.debug(
        "Building ebean datasource (shared AWS credentials present={})",
        defaultAwsCredentialsProvider != null);
    return buildDataSourceConfig(ebeanDatasourceUrl, metricUtils);
  }

  public DataSourceConfig buildDataSourceConfig(String dataSourceUrl, MetricUtils metricUtils) {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();

    // Configure cross-cloud IAM authentication
    boolean shouldUseIam = useIamAuth || postgresUseIamAuth;

    CrossCloudIamUtils.CrossCloudConfig crossCloudConfig =
        CrossCloudIamUtils.configureCrossCloudIam(
            dataSourceUrl,
            ebeanDatasourceDriver,
            shouldUseIam,
            cloudProvider,
            awsRegion,
            awsAccessKeyId,
            awsSecretAccessKey,
            awsSessionToken,
            googleApplicationCredentials,
            gcpProject,
            instanceConnectionName);

    dataSourceConfig.setUsername(ebeanDatasourceUsername);
    dataSourceConfig.setPassword(ebeanDatasourcePassword);
    dataSourceConfig.setUrl(crossCloudConfig.url);
    dataSourceConfig.setDriver(crossCloudConfig.driver);
    dataSourceConfig.setMinConnections(ebeanMinConnections);
    dataSourceConfig.setMaxConnections(ebeanMaxConnections);
    dataSourceConfig.setMaxInactiveTimeSecs(ebeanMaxInactiveTimeSecs);
    dataSourceConfig.setMaxAgeMinutes(ebeanMaxAgeMinutes);
    dataSourceConfig.setLeakTimeMinutes(ebeanLeakTimeMinutes);
    dataSourceConfig.setWaitTimeoutMillis(ebeanWaitTimeoutMillis);
    dataSourceConfig.setAutoCommit(ebeanAutoCommit);
    dataSourceConfig.setListener(getListenerToTrackCounts(metricUtils, "main"));
    EbeanPoolDefaults.applyDefaultTransactionIsolation(dataSourceConfig);

    // Set custom properties for IAM authentication
    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Bean(name = "gmsEbeanDatabaseConfig")
  protected DatabaseConfig createInstance(
      @Qualifier("ebeanDataSourceConfig") DataSourceConfig config,
      List<EbeanConfigCustomizer> customizers) {
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("gmsEbeanDatabaseConfig");
    serverConfig.setDataSourceConfig(config);
    serverConfig.setDdlGenerate(ebeanAutoCreate);
    serverConfig.setDdlRun(ebeanAutoCreate);
    customizers.forEach(customizer -> customizer.customize(serverConfig));
    ActorSqlComment.install(serverConfig, "gmsEbeanDatabaseConfig", config, postgresActorComment);
    return serverConfig;
  }

  /**
   * Resolves the Postgres backend process id of a pooled connection via the driver's {@code
   * PGConnection#getBackendPID()}, looked up reflectively because the driver is a runtime-only
   * dependency and the store may not be Postgres at all. Returns -1 when unavailable, and stops
   * trying after the first failure so non-Postgres installs pay one lookup per JVM.
   */
  static final class PgBackendPid {
    private static final java.util.concurrent.atomic.AtomicBoolean SUPPORTED =
        new java.util.concurrent.atomic.AtomicBoolean(true);
    private static volatile Class<?> pgConnection;
    private static volatile java.lang.reflect.Method getBackendPid;

    private PgBackendPid() {}

    static long of(Connection connection) {
      if (!SUPPORTED.get() || connection == null) {
        return -1L;
      }
      try {
        Class<?> iface = pgConnection;
        if (iface == null) {
          iface = Class.forName("org.postgresql.PGConnection");
          getBackendPid = iface.getMethod("getBackendPID");
          pgConnection = iface;
        }
        if (!connection.isWrapperFor(iface)) {
          SUPPORTED.set(false);
          return -1L;
        }
        Object pg = connection.unwrap(iface);
        return ((Number) getBackendPid.invoke(pg)).longValue();
      } catch (Throwable t) {
        SUPPORTED.set(false);
        return -1L;
      }
    }
  }
}
