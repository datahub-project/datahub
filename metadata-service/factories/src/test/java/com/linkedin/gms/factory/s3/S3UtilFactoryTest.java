package com.linkedin.gms.factory.s3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.metadata.utils.aws.S3Util;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.SkipException;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

@Configuration
class TestStsClientConfiguration {

  @Bean
  @Primary
  public StsClient mockStsClient() {
    StsClient mockClient = org.mockito.Mockito.mock(StsClient.class);

    // Mock STS assumeRole to return fake credentials with future expiry
    Credentials mockCredentials =
        Credentials.builder()
            .accessKeyId("test-access-key")
            .secretAccessKey("test-secret-key")
            .sessionToken("test-session-token")
            .expiration(Instant.now().plusSeconds(3600)) // 1 hour from now
            .build();

    AssumeRoleResponse mockResponse =
        AssumeRoleResponse.builder().credentials(mockCredentials).build();

    when(mockClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);

    return mockClient;
  }
}

/**
 * Tests S3Util creation without role ARN. Uses explicit overrides so behavior is the same in or
 * outside AWS: aws.region + dummy AWS_ENDPOINT_URL (system property) avoid real AWS calls.
 */
@SpringBootTest(classes = {S3UtilFactory.class, TestStsClientConfiguration.class})
@TestPropertySource(properties = {"datahub.s3.roleArn="})
public class S3UtilFactoryTest extends AbstractTestNGSpringContextTests {

  static {
    System.setProperty("aws.region", "us-east-1");
    System.setProperty("AWS_ENDPOINT_URL", "http://localhost:9999");
  }

  @Autowired
  @Qualifier("s3Util")
  private S3Util s3Util;

  @Test
  public void testS3UtilCreationWithoutRoleArn() {
    // When/Then
    assertNotNull(s3Util, "S3Util bean should be created successfully");
  }

  @Test
  public void testS3UtilBeanName() {
    // Verify the bean is registered with the correct name
    assertNotNull(s3Util, "S3Util bean should be available with name 's3Util'");
  }
}

/**
 * Tests S3Util creation with STS role ARN. Mock STS is used; aws.region is set so the test works in
 * or outside AWS.
 */
@SpringBootTest(classes = {S3UtilFactory.class, TestStsClientConfiguration.class})
@TestPropertySource(properties = {"datahub.s3.roleArn=arn:aws:iam::123456789012:role/test-role"})
class S3UtilFactoryWithRoleArnTest extends AbstractTestNGSpringContextTests {

  static {
    System.setProperty("aws.region", "us-east-1");
  }

  @Autowired
  @Qualifier("s3Util")
  private S3Util s3Util;

  @Test
  public void testS3UtilCreationWithRoleArn() {
    // When/Then
    assertNotNull(s3Util, "S3Util bean should be created successfully with STS role ARN");
  }
}

/** Tests S3Util with endpoint but no region (covers clientBuilder.region(US_EAST_1) branch). */
@SpringBootTest(classes = {S3UtilFactory.class})
@TestPropertySource(properties = {"datahub.s3.roleArn="})
@ContextConfiguration(initializers = EndpointOnlyForS3Initializer.class)
class S3UtilFactoryEndpointOnlyTest extends AbstractTestNGSpringContextTests {

  @Autowired(required = false)
  @Qualifier("s3Util")
  private S3Util s3Util;

  @Test
  public void testS3UtilCreatedWithEndpointOnlyNoRegion() {
    assertNotNull(
        s3Util,
        "S3Util bean should be created when only AWS_ENDPOINT_URL set (region defaulted to US_EAST_1)");
  }
}

/**
 * Clears AWS-related system properties before the context loads so S3UtilFactory sees no config
 * (other test classes in the same JVM may have set aws.region / AWS_ENDPOINT_URL).
 */
final class ClearAwsPropertiesInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    System.clearProperty("aws.region");
    System.clearProperty("AWS_REGION");
    System.clearProperty("AWS_ENDPOINT_URL");
  }
}

/** Endpoint only (no region) so factory hits clientBuilder.region(Region.US_EAST_1) branch. */
final class EndpointOnlyForS3Initializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    System.clearProperty("aws.region");
    System.clearProperty("AWS_REGION");
    System.setProperty("AWS_ENDPOINT_URL", "http://localhost:9999");
  }
}

/**
 * Verifies S3Util is not created when no AWS config is present (quickstart/local dev: no
 * datahub.s3.roleArn, no AWS_ENDPOINT_URL, no AWS_REGION, no aws.region).
 *
 * <p>Uses ClearAwsPropertiesInitializer so properties are cleared before the factory runs,
 * regardless of test order. When run inside AWS (e.g. CI/EC2), env vars AWS_REGION or
 * AWS_ENDPOINT_URL may be set and cannot be overridden; the test skips in that case.
 */
@SpringBootTest(classes = {S3UtilFactory.class})
@ContextConfiguration(initializers = ClearAwsPropertiesInitializer.class)
@TestPropertySource(properties = {"datahub.s3.roleArn="})
class S3UtilFactoryNoAwsConfigTest extends AbstractTestNGSpringContextTests {

  @Autowired(required = false)
  @Qualifier("s3Util")
  private S3Util s3Util;

  @Test
  public void testS3UtilIsNullWhenNoAwsConfig() {
    String awsRegion = System.getenv("AWS_REGION");
    String awsEndpoint = System.getenv("AWS_ENDPOINT_URL");
    if ((awsRegion != null && !awsRegion.isEmpty())
        || (awsEndpoint != null && !awsEndpoint.isEmpty())) {
      throw new SkipException(
          "Skipping: running inside AWS (AWS_REGION or AWS_ENDPOINT_URL set in env); "
              + "cannot simulate no-config. S3Util would be created by the factory.");
    }
    assertNull(s3Util, "S3Util bean should be null when no AWS region or endpoint is configured");
  }
}
