package com.linkedin.gms.factory.common;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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
    classes = {LocalEbeanConfigFactory.class, LocalEbeanConfigFactoryAwsTest.TestConfig.class})
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
      // AWS-specific environment variables
      "AWS_REGION=us-west-2",
      "AWS_ACCESS_KEY_ID=test-access-key",
      "AWS_SECRET_ACCESS_KEY=test-secret-key",
      "AWS_SESSION_TOKEN=test-session-token",
      // Explicitly set GCP environment variables to null to prevent contamination
      "GOOGLE_APPLICATION_CREDENTIALS=",
      "GCP_PROJECT=",
      "INSTANCE_CONNECTION_NAME="
    })
public class LocalEbeanConfigFactoryAwsTest extends AbstractTestNGSpringContextTests {

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
    // since AWS tests need to control IAM auth settings per test
    ReflectionTestUtils.setField(localEbeanConfigFactory, "cloudProvider", "auto");
  }

  @Test
  public void testDetectCloudProviderAws() {
    String awsResult =
        CrossCloudIamUtils.detectCloudProvider(
            "jdbc:mysql://localhost:3306/datahub",
            "auto",
            "us-west-2", // AWS_REGION
            null,
            null,
            null,
            null,
            null,
            null);
    assertEquals(awsResult, "aws");
  }

  @Test
  public void testDetectCloudProviderAwsUrl() {
    String urlResult =
        CrossCloudIamUtils.detectCloudProvider(
            "jdbc:mysql://rds.amazonaws.com:3306/datahub",
            "auto",
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    assertEquals(urlResult, "aws");
  }

  @Test
  public void testBuildDataSourceConfigWithIamAuth() {
    // Override non-cloud properties using reflection
    ReflectionTestUtils.setField(localEbeanConfigFactory, "useIamAuth", true);
    ReflectionTestUtils.setField(localEbeanConfigFactory, "cloudProvider", "aws");

    String dataSourceUrl = "jdbc:mysql://localhost:3306/datahub";

    DataSourceConfig result =
        localEbeanConfigFactory.buildDataSourceConfig(dataSourceUrl, mockMetricUtils);

    assertNotNull(result);
    assertEquals(result.getUsername(), "testuser");
    assertEquals(result.getPassword(), "testpass");
    // URL should be modified for AWS IAM using AWS JDBC Wrapper
    assertTrue(result.getUrl().contains("jdbc:aws-wrapper:mysql://"));
    assertTrue(result.getUrl().contains("wrapperPlugins=iam"));
    assertEquals(result.getDriver(), "software.amazon.jdbc.Driver");
  }

  @Test
  public void testBuildDataSourceConfigWithPostgresIam() {
    // Override non-cloud properties using reflection
    ReflectionTestUtils.setField(localEbeanConfigFactory, "postgresUseIamAuth", true);
    ReflectionTestUtils.setField(localEbeanConfigFactory, "cloudProvider", "aws");

    String dataSourceUrl = "jdbc:postgresql://localhost:5432/datahub";

    DataSourceConfig result =
        localEbeanConfigFactory.buildDataSourceConfig(dataSourceUrl, mockMetricUtils);

    assertNotNull(result);
    assertEquals(result.getUsername(), "testuser");
    assertEquals(result.getPassword(), "testpass");
    // For PostgreSQL with AWS IAM, URL and driver remain unchanged, only custom properties are set
    assertEquals(result.getUrl(), dataSourceUrl); // URL should remain unchanged
    assertEquals(
        result.getDriver(), "com.mysql.cj.jdbc.Driver"); // Driver should remain as configured

    // Check that IAM custom properties are set
    Map<String, String> customProperties = result.getCustomProperties();
    assertNotNull(customProperties);
    assertEquals(customProperties.get("wrapperPlugins"), "iam");
  }

  @Test
  public void testConfigureCrossCloudIamWithAwsIam() {
    String originalUrl = "jdbc:mysql://localhost:3306/datahub";

    CrossCloudIamUtils.CrossCloudConfig result =
        CrossCloudIamUtils.configureCrossCloudIam(
            originalUrl,
            "com.mysql.cj.jdbc.Driver",
            true,
            "aws",
            "us-west-2",
            null,
            null,
            null,
            null,
            null,
            null);

    assertNotNull(result);
    assertEquals(result.driver, "software.amazon.jdbc.Driver");
    assertTrue(result.url.contains("jdbc:aws-wrapper:mysql://"));
    assertTrue(result.url.contains("wrapperPlugins=iam"));
  }
}
