package com.linkedin.gms.factory.aws;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

/**
 * Covers object-storage S3 client construction paths that used to live in
 * ObjectStorageClientFactoryTest (endpoint-only and role-arn assume-role).
 */
public class AwsClientFactoryObjectStorageTest {

  @Mock private ConfigurationProvider configurationProvider;

  private AwsClientFactory awsClientFactory;
  private AutoCloseable mocks;
  private DataHubConfiguration dataHubConfiguration;

  @BeforeMethod
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    awsClientFactory = new AwsClientFactory();
    ReflectionTestUtils.setField(awsClientFactory, "configurationProvider", configurationProvider);

    dataHubConfiguration = new DataHubConfiguration();
    dataHubConfiguration.setObjectStorage(new ObjectStorageConfiguration());
    when(configurationProvider.getDatahub()).thenReturn(dataHubConfiguration);

    System.clearProperty("AWS_REGION");
    System.clearProperty("AWS_ENDPOINT_URL");
    System.clearProperty("aws.region");
  }

  @AfterMethod
  public void tearDown() throws Exception {
    System.clearProperty("AWS_REGION");
    System.clearProperty("AWS_ENDPOINT_URL");
    System.clearProperty("aws.region");
    awsClientFactory.shutdown();
    if (mocks != null) {
      mocks.close();
    }
  }

  private static void skipIfAwsEnvironmentConfigured() {
    if (AwsClientFactory.isAwsConfigured()) {
      throw new SkipException(
          "AWS_REGION/AWS_ENDPOINT_URL/aws.region present; cannot assert non-AWS skip paths");
    }
  }

  @Test
  public void skipsS3ClientWhenNoAwsConfig() {
    skipIfAwsEnvironmentConfigured();
    S3Client client = awsClientFactory.objectStorageS3Client(null);
    assertNull(client);
  }

  @Test
  public void createsS3ClientFromEndpointOnly() {
    System.setProperty("AWS_ENDPOINT_URL", "http://localhost:9999");

    S3Client client = awsClientFactory.objectStorageS3Client(null);
    assertNotNull(client);
  }

  @Test
  public void createsS3ClientWithRoleCredentialsAndRegion() {
    dataHubConfiguration.getObjectStorage().setRoleArn("arn:aws:iam::123456789012:role/test-role");
    System.setProperty("aws.region", "us-east-1");

    AwsCredentialsProvider roleCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("test-key", "test-secret"));

    S3Client client = awsClientFactory.objectStorageS3Client(roleCredentials);
    assertNotNull(client);
  }

  @Test
  public void roleArnWithoutStsReturnsNullCredentialsInNonAws() {
    skipIfAwsEnvironmentConfigured();
    dataHubConfiguration.getObjectStorage().setRoleArn("arn:aws:iam::123456789012:role/test-role");

    AwsCredentialsProvider provider =
        awsClientFactory.objectStorageCredentialsProvider(/* defaultProvider= */ null, null);
    assertNull(provider);
  }

  @Test
  public void roleArnWithRegionButMissingCredentialsSoftSkips() {
    dataHubConfiguration.getObjectStorage().setRoleArn("arn:aws:iam::123456789012:role/test-role");
    System.setProperty("aws.region", "us-east-1");

    S3Client client = awsClientFactory.objectStorageS3Client(null);
    assertNull(client);
  }

  @Test
  public void roleArnWithoutRegionSkipsS3ClientInNonAws() {
    skipIfAwsEnvironmentConfigured();
    dataHubConfiguration.getObjectStorage().setRoleArn("arn:aws:iam::123456789012:role/test-role");

    S3Client client = awsClientFactory.objectStorageS3Client(null);
    assertNull(client);
  }

  @Test
  public void objectStorageCredentialsProviderUsesStsWhenAvailable() {
    dataHubConfiguration.getObjectStorage().setRoleArn("arn:aws:iam::123456789012:role/test-role");
    StsClient stsClient = mock(StsClient.class);

    AwsCredentialsProvider provider =
        awsClientFactory.objectStorageCredentialsProvider(/* defaultProvider= */ null, stsClient);
    assertNotNull(provider);
  }
}
