package com.linkedin.gms.factory.common;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.datasource.DataSourceConfig;
import java.sql.Connection;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = {LocalEbeanConfigFactory.class, LocalEbeanConfigFactoryGcpTest.TestConfig.class})
@TestPropertySource(
    properties = {
      "ebean.username=testuser",
      "ebean.password=testpass",
      "ebean.driver=com.mysql.cj.jdbc.Driver",
      "ebean.url=jdbc:mysql://localhost:3306/datahub",
      "ebean.minConnections=2",
      "ebean.maxConnections=50",
      "ebean.maxInactiveTimeSeconds=120",
      "ebean.maxAgeMinutes=120",
      "ebean.leakTimeMinutes=15",
      "ebean.waitTimeoutMillis=1000",
      "ebean.autoCreateDdl=false",
      // GCP-specific environment variables
      "GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json",
      "GCP_PROJECT=test-project",
      "INSTANCE_CONNECTION_NAME=project:region:instance",
      // Explicitly set AWS environment variables to null to prevent contamination
      "AWS_REGION=",
      "AWS_ACCESS_KEY_ID=",
      "AWS_SECRET_ACCESS_KEY=",
      "AWS_SESSION_TOKEN="
    })
public class LocalEbeanConfigFactoryGcpTest extends AbstractTestNGSpringContextTests {

  @Mock private DataSourceConfig mockDataSourceConfig;
  @Mock private Connection mockConnection;

  @Autowired private LocalEbeanConfigFactory localEbeanConfigFactory;
  @Autowired private MetricUtils mockMetricUtils;

  @Configuration
  static class TestConfig {
    @Bean
    public MetricUtils metricUtils() {
      return mock(MetricUtils.class);
    }
  }

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Reset only cloudProvider to auto, but keep IAM auth flags as they are
    // since GCP tests need to control IAM auth settings per test
    ReflectionTestUtils.setField(localEbeanConfigFactory, "cloudProvider", "auto");
  }

  @Test
  public void testDetectCloudProviderGcp() {
    String gcpResult =
        CrossCloudIamUtils.detectCloudProvider(
            "jdbc:mysql://localhost:3306/datahub",
            "auto",
            null,
            null,
            null,
            null,
            "/path/to/service-account.json", // GOOGLE_APPLICATION_CREDENTIALS
            null,
            null);
    assertEquals(gcpResult, "gcp");
  }

  @Test
  public void testBuildDataSourceConfigWithGcpMysql() {
    // Override non-cloud properties using reflection
    ReflectionTestUtils.setField(localEbeanConfigFactory, "useIamAuth", true);
    ReflectionTestUtils.setField(localEbeanConfigFactory, "cloudProvider", "gcp");

    String dataSourceUrl = "jdbc:mysql://localhost:3306/datahub";

    DataSourceConfig result =
        localEbeanConfigFactory.buildDataSourceConfig(dataSourceUrl, mockMetricUtils);

    assertNotNull(result);
    assertEquals(result.getUsername(), "testuser");
    assertEquals(result.getPassword(), "testpass");
    assertEquals(result.getDriver(), "com.google.cloud.sql.mysql.SocketFactory");
    assertEquals(result.getUrl(), dataSourceUrl);

    Map<String, String> customProperties = result.getCustomProperties();
    assertNotNull(customProperties);
    assertEquals(customProperties.get("socketFactory"), "com.google.cloud.sql.mysql.SocketFactory");
    assertEquals(customProperties.get("cloudSqlInstance"), "project:region:instance");
    assertEquals(customProperties.get("enableIamAuth"), "true");
    assertEquals(customProperties.get("sslmode"), "disable");
    assertEquals(customProperties.get("cloudSqlRefreshStrategy"), "lazy");
  }

  @Test
  public void testBuildDataSourceConfigWithGcpPostgres() {
    // Override non-cloud properties using reflection
    ReflectionTestUtils.setField(localEbeanConfigFactory, "postgresUseIamAuth", true);
    ReflectionTestUtils.setField(localEbeanConfigFactory, "cloudProvider", "gcp");

    String dataSourceUrl = "jdbc:postgresql://localhost:5432/datahub";

    DataSourceConfig result =
        localEbeanConfigFactory.buildDataSourceConfig(dataSourceUrl, mockMetricUtils);

    assertNotNull(result);
    assertEquals(result.getUsername(), "testuser");
    assertEquals(result.getPassword(), "testpass");
    assertEquals(result.getDriver(), "com.google.cloud.sql.postgres.SocketFactory");
    assertEquals(result.getUrl(), dataSourceUrl);

    Map<String, String> customProperties = result.getCustomProperties();
    assertNotNull(customProperties);
    assertEquals(
        customProperties.get("socketFactory"), "com.google.cloud.sql.postgres.SocketFactory");
    assertEquals(customProperties.get("cloudSqlInstance"), "project:region:instance");
    assertEquals(customProperties.get("enableIamAuth"), "true");
    assertEquals(customProperties.get("sslmode"), "disable");
    assertEquals(customProperties.get("cloudSqlRefreshStrategy"), "lazy");
  }

  @Test
  public void testConfigureCrossCloudIamWithGcpIam() {
    String originalUrl = "jdbc:mysql://localhost:3306/datahub";

    CrossCloudIamUtils.CrossCloudConfig result =
        CrossCloudIamUtils.configureCrossCloudIam(
            originalUrl,
            "com.mysql.cj.jdbc.Driver",
            true,
            "gcp",
            null,
            null,
            null,
            null,
            "/path/to/service-account.json",
            null,
            "project:region:instance");

    assertNotNull(result);
    assertEquals(result.driver, "com.google.cloud.sql.mysql.SocketFactory");
    assertEquals(result.url, originalUrl);

    Map<String, String> customProperties = result.customProperties;
    assertNotNull(customProperties);
    assertEquals(customProperties.get("socketFactory"), "com.google.cloud.sql.mysql.SocketFactory");
    assertEquals(customProperties.get("cloudSqlInstance"), "project:region:instance");
    assertEquals(customProperties.get("enableIamAuth"), "true");
    assertEquals(customProperties.get("sslmode"), "disable");
    assertEquals(customProperties.get("cloudSqlRefreshStrategy"), "lazy");
  }
}
