package com.linkedin.gms.factory.s3;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.SkipException;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.sts.StsClient;

/**
 * Clears AWS-related system properties before the context loads so StsClientFactory sees no config.
 */
final class ClearAwsPropertiesForStsInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    System.clearProperty("aws.region");
    System.clearProperty("AWS_REGION");
    System.clearProperty("AWS_ENDPOINT_URL");
  }
}

/** Sets aws.region + endpoint so the factory creates a client (endpoint path, works in CI). */
final class SetAwsRegionAndEndpointForStsInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    System.setProperty("aws.region", "us-east-1");
    System.setProperty("AWS_ENDPOINT_URL", "http://localhost:4566");
  }
}

/** Sets only AWS_ENDPOINT_URL so the factory uses custom endpoint + dummy credentials. */
final class SetAwsEndpointForStsInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    System.setProperty("AWS_ENDPOINT_URL", "http://localhost:4566");
  }
}

/** Tests StsClient creation when region and endpoint are set (deterministic in CI). */
@SpringBootTest(classes = {StsClientFactory.class})
@ContextConfiguration(initializers = SetAwsRegionAndEndpointForStsInitializer.class)
class StsClientFactoryWithRegionTest extends AbstractTestNGSpringContextTests {

  @Autowired(required = false)
  @Qualifier("stsClient")
  private StsClient stsClient;

  @Test
  public void testStsClientCreatedWhenRegionSet() {
    assertNotNull(stsClient, "StsClient bean should be created when aws.region (and endpoint) set");
  }
}

/** Tests StsClient creation with custom endpoint (LocalStack/custom endpoint path). */
@SpringBootTest(classes = {StsClientFactory.class})
@ContextConfiguration(initializers = SetAwsEndpointForStsInitializer.class)
class StsClientFactoryWithEndpointTest extends AbstractTestNGSpringContextTests {

  @Autowired(required = false)
  @Qualifier("stsClient")
  private StsClient stsClient;

  @Test
  public void testStsClientCreatedWithCustomEndpoint() {
    assertNotNull(
        stsClient, "StsClient bean should be created with custom endpoint and dummy credentials");
  }
}

/** Sets invalid endpoint so URI.create() throws and factory catch block (log.error path) runs. */
final class InvalidEndpointForStsInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    System.setProperty("AWS_ENDPOINT_URL", "not a valid uri");
  }
}

/** Tests StsClient is null when factory throws (covers catch block / exception path). */
@SpringBootTest(classes = {StsClientFactory.class})
@ContextConfiguration(initializers = InvalidEndpointForStsInitializer.class)
class StsClientFactoryExceptionTest extends AbstractTestNGSpringContextTests {

  @Autowired(required = false)
  @Qualifier("stsClient")
  private StsClient stsClient;

  @Test
  public void testStsClientNullWhenException() {
    assertNull(
        stsClient, "StsClient bean should be null when factory throws (e.g. invalid endpoint URI)");
  }
}

/** Tests StsClient is null when no AWS config is present. */
@SpringBootTest(classes = {StsClientFactory.class})
@ContextConfiguration(initializers = ClearAwsPropertiesForStsInitializer.class)
class StsClientFactoryNoAwsConfigTest extends AbstractTestNGSpringContextTests {

  @Autowired(required = false)
  @Qualifier("stsClient")
  private StsClient stsClient;

  @Test
  public void testStsClientNullWhenNoAwsConfig() {
    String awsRegion = System.getenv("AWS_REGION");
    String awsEndpoint = System.getenv("AWS_ENDPOINT_URL");
    if ((awsRegion != null && !awsRegion.isEmpty())
        || (awsEndpoint != null && !awsEndpoint.isEmpty())) {
      throw new SkipException(
          "Skipping: AWS_REGION or AWS_ENDPOINT_URL set in env; cannot simulate no-config");
    }
    assertNull(stsClient, "StsClient bean should be null when no AWS region or endpoint is set");
  }
}
