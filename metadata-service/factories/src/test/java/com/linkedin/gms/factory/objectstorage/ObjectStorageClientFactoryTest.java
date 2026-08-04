package com.linkedin.gms.factory.objectstorage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import com.linkedin.metadata.utils.objectstorage.GcsObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.LocalObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageProvider;
import com.linkedin.metadata.utils.objectstorage.S3ObjectStorageClient;
import java.time.Instant;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class ObjectStorageClientFactoryTest {

  @Mock private ConfigurationProvider configurationProvider;

  private ObjectStorageClientFactory factory;
  private DataHubConfiguration dataHubConfiguration;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    factory = new ObjectStorageClientFactory();
    ReflectionTestUtils.setField(factory, "configurationProvider", configurationProvider);

    dataHubConfiguration = new DataHubConfiguration();
    ObjectStorageConfiguration objectStorageConfiguration = new ObjectStorageConfiguration();
    objectStorageConfiguration.setBucket("");
    dataHubConfiguration.setObjectStorage(objectStorageConfiguration);

    when(configurationProvider.getDatahub()).thenReturn(dataHubConfiguration);
  }

  @AfterMethod
  public void clearAwsProperties() {
    System.clearProperty("aws.region");
    System.clearProperty("AWS_REGION");
    System.clearProperty("AWS_ENDPOINT_URL");
  }

  @Test
  public void testCreatesLocalClientFromUri() {
    dataHubConfiguration.getObjectStorage().setUri("file:///tmp/datahub-object-storage");

    ObjectStorageClient client = factory.getInstance();
    assertNotNull(client);
    assertTrue(client instanceof LocalObjectStorageClient);
    assertTrue(client.isConfigured());
    assertTrue(client.provider() == ObjectStorageProvider.LOCAL);
  }

  @Test
  public void testCreatesLocalClientFromLegacyConfig() {
    dataHubConfiguration.getObjectStorage().setPath("/tmp/datahub-object-storage");
    dataHubConfiguration.getObjectStorage().setProvider("local");

    ObjectStorageClient client = factory.getInstance();
    assertNotNull(client);
    assertTrue(client instanceof LocalObjectStorageClient);
    assertTrue(client.isConfigured());
    assertTrue(client.provider() == ObjectStorageProvider.LOCAL);
  }

  @Test
  public void testCreatesS3ClientFromUriWithAwsRegion() {
    dataHubConfiguration.getObjectStorage().setUri("s3://my-bucket/prefix");
    System.setProperty("aws.region", "us-east-1");

    ObjectStorageClient client = factory.getInstance();
    assertNotNull(client);
    assertTrue(client instanceof S3ObjectStorageClient);
    assertTrue(client.provider() == ObjectStorageProvider.S3);
  }

  @Test
  public void testCreatesS3ClientFromUriWithEndpointOnly() {
    dataHubConfiguration.getObjectStorage().setUri("s3://my-bucket");
    System.setProperty("AWS_ENDPOINT_URL", "http://localhost:9999");

    ObjectStorageClient client = factory.getInstance();
    assertNotNull(client);
    assertTrue(client instanceof S3ObjectStorageClient);
  }

  @Test
  public void testCreatesS3ClientWithRoleArn() {
    dataHubConfiguration.getObjectStorage().setUri("s3://my-bucket");
    dataHubConfiguration.getObjectStorage().setRoleArn("arn:aws:iam::123456789012:role/test-role");
    System.setProperty("aws.region", "us-east-1");

    StsClient stsClient = mock(StsClient.class);
    Credentials credentials =
        Credentials.builder()
            .accessKeyId("test-access-key")
            .secretAccessKey("test-secret-key")
            .sessionToken("test-session-token")
            .expiration(Instant.now().plusSeconds(3600))
            .build();
    when(stsClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(AssumeRoleResponse.builder().credentials(credentials).build());
    ReflectionTestUtils.setField(factory, "stsClient", stsClient);

    ObjectStorageClient client = factory.getInstance();
    assertNotNull(client);
    assertTrue(client instanceof S3ObjectStorageClient);
  }

  @Test
  public void testCreatesGcsClientFromUri() {
    dataHubConfiguration.getObjectStorage().setUri("gs://my-bucket/prefix");

    ObjectStorageClient client = factory.getInstance();
    assertNotNull(client);
    assertTrue(client instanceof GcsObjectStorageClient);
    assertTrue(client.provider() == ObjectStorageProvider.GCS);
  }

  @Test
  public void testReturnsNullWhenS3LocationWithoutAwsConfig() {
    if (hasAwsEnvironmentConfig()) {
      throw new SkipException("AWS env vars are set; cannot assert null S3 client");
    }
    dataHubConfiguration.getObjectStorage().setUri("s3://my-bucket");

    ObjectStorageClient client = factory.getInstance();
    assertNull(client);
  }

  private static boolean hasAwsEnvironmentConfig() {
    String awsRegion = System.getenv("AWS_REGION");
    if (awsRegion != null && !awsRegion.isBlank()) {
      return true;
    }
    String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
    return endpointUrl != null && !endpointUrl.isBlank();
  }

  @Test
  public void testReturnsNullWhenLocationUnconfigured() {
    ObjectStorageClient client = factory.getInstance();
    assertNull(client);
  }

  @Test
  public void testReturnsNullWhenLegacyLocalPathMissing() {
    dataHubConfiguration.getObjectStorage().setProvider("local");
    ObjectStorageClient client = factory.getInstance();
    assertNull(client);
  }
}
