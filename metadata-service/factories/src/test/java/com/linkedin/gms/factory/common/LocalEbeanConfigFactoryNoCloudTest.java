package com.linkedin.gms.factory.common;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.datasource.DataSourceConfig;
import io.ebean.datasource.DataSourcePoolListener;
import java.sql.Connection;
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
    classes = {LocalEbeanConfigFactory.class, LocalEbeanConfigFactoryNoCloudTest.TestConfig.class})
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
      // Explicitly override all cloud environment variables to null to prevent contamination
      "AWS_REGION=",
      "AWS_ACCESS_KEY_ID=",
      "AWS_SECRET_ACCESS_KEY=",
      "AWS_SESSION_TOKEN=",
      "GOOGLE_APPLICATION_CREDENTIALS=",
      "GCP_PROJECT=",
      "INSTANCE_CONNECTION_NAME="
    })
public class LocalEbeanConfigFactoryNoCloudTest extends AbstractTestNGSpringContextTests {

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

    // Reset all non-cloud properties to defaults using reflection
    ReflectionTestUtils.setField(localEbeanConfigFactory, "useIamAuth", false);
    ReflectionTestUtils.setField(localEbeanConfigFactory, "postgresUseIamAuth", false);
    ReflectionTestUtils.setField(localEbeanConfigFactory, "cloudProvider", "auto");
  }

  @Test
  public void testDetectCloudProviderTraditional() {
    // No cloud environment variables should result in "traditional"
    String traditionalResult =
        localEbeanConfigFactory.detectCloudProvider("jdbc:mysql://localhost:3306/datahub");
    assertEquals(traditionalResult, "traditional");
  }

  @Test
  public void testDetectCloudProviderAuto() {
    // Test cloud provider detection when set to "auto" (default)
    // With no cloud environment variables and localhost URL, should detect "traditional"
    String result =
        localEbeanConfigFactory.detectCloudProvider("jdbc:mysql://localhost:3306/datahub");
    assertEquals(result, "traditional"); // Should detect traditional when no cloud indicators
  }

  @Test
  public void testBuildDataSourceConfig() {
    String dataSourceUrl = "jdbc:mysql://localhost:3306/datahub";

    DataSourceConfig result =
        localEbeanConfigFactory.buildDataSourceConfig(dataSourceUrl, mockMetricUtils);

    assertNotNull(result);
    assertEquals(result.getUsername(), "testuser");
    assertEquals(result.getPassword(), "testpass");
    assertEquals(result.getDriver(), "com.mysql.cj.jdbc.Driver");
    assertEquals(result.getUrl(), dataSourceUrl);
    assertEquals(result.getMinConnections(), Integer.valueOf(2));
    assertEquals(result.getMaxConnections(), Integer.valueOf(50));
    assertEquals(result.getMaxInactiveTimeSecs(), Integer.valueOf(120));
    assertEquals(result.getMaxAgeMinutes(), Integer.valueOf(120));
    assertEquals(result.getLeakTimeMinutes(), Integer.valueOf(15));
    assertEquals(result.getWaitTimeoutMillis(), Integer.valueOf(1000));
  }

  @Test
  public void testConfigureCrossCloudIam() {
    String originalUrl = "jdbc:mysql://localhost:3306/datahub";

    LocalEbeanConfigFactory.CrossCloudConfig result =
        localEbeanConfigFactory.configureCrossCloudIam(originalUrl);

    assertNotNull(result);
    assertEquals(result.driver, "com.mysql.cj.jdbc.Driver");
    assertEquals(result.url, originalUrl);
    assertEquals(result.customProperties, null);
  }

  @Test
  public void testConfigureCrossCloudIamWithUnsupportedCloud() {
    // Override non-cloud properties using reflection
    ReflectionTestUtils.setField(localEbeanConfigFactory, "useIamAuth", true);
    ReflectionTestUtils.setField(localEbeanConfigFactory, "cloudProvider", "azure");

    String originalUrl = "jdbc:mysql://localhost:3306/datahub";

    LocalEbeanConfigFactory.CrossCloudConfig result =
        localEbeanConfigFactory.configureCrossCloudIam(originalUrl);

    assertNotNull(result);
    assertEquals(result.driver, "com.mysql.cj.jdbc.Driver");
    assertEquals(result.url, originalUrl);
    assertEquals(result.customProperties, null);
  }

  @Test
  public void testIsPostgresUrl() {
    // Test PostgreSQL URL
    boolean result =
        localEbeanConfigFactory.isPostgresUrl("jdbc:postgresql://localhost:5432/datahub");
    assertTrue(result);

    // Test MySQL URL
    boolean mysqlResult =
        localEbeanConfigFactory.isPostgresUrl("jdbc:mysql://localhost:3306/datahub");
    assertTrue(!mysqlResult);

    // Test null URL
    boolean nullResult = localEbeanConfigFactory.isPostgresUrl((String) null);
    assertTrue(!nullResult);
  }

  @Test
  public void testIsMysqlUrl() {
    // Test MySQL URL
    boolean result = localEbeanConfigFactory.isMysqlUrl("jdbc:mysql://localhost:3306/datahub");
    assertTrue(result);

    // Test PostgreSQL URL
    boolean postgresResult =
        localEbeanConfigFactory.isMysqlUrl("jdbc:postgresql://localhost:5432/datahub");
    assertTrue(!postgresResult);

    // Test null URL
    boolean nullResult = localEbeanConfigFactory.isMysqlUrl((String) null);
    assertTrue(!nullResult);
  }

  @Test
  public void testGetListenerToTrackCounts() {
    DataSourcePoolListener listener =
        LocalEbeanConfigFactory.getListenerToTrackCounts(mockMetricUtils, "test");

    assertNotNull(listener);

    // Test the listener methods - should not throw exception
    listener.onAfterBorrowConnection(mockConnection);
    listener.onBeforeReturnConnection(mockConnection);
  }

  @Test
  public void testGetListenerToTrackCountsWithNullMetricUtils() {
    DataSourcePoolListener listener =
        LocalEbeanConfigFactory.getListenerToTrackCounts(null, "test");

    assertNotNull(listener);

    // Test the listener methods - should not throw exception
    listener.onAfterBorrowConnection(mockConnection);
    listener.onBeforeReturnConnection(mockConnection);
  }
}
