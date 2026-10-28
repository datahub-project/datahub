package com.linkedin.gms.factory.objectstorage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import com.linkedin.metadata.utils.objectstorage.GcsObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.LocalObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageLocation;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageProvider;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageReference;
import com.linkedin.metadata.utils.objectstorage.S3ObjectStorageClient;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

public class ObjectStorageClientFactoryTest {

  @Mock private ConfigurationProvider configurationProvider;

  private ObjectStorageClientFactory factory;
  private DataHubConfiguration dataHubConfiguration;
  private S3Client sharedS3Client;
  private S3Presigner sharedS3Presigner;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    factory = new ObjectStorageClientFactory();
    ReflectionTestUtils.setField(factory, "configurationProvider", configurationProvider);

    sharedS3Client = mock(S3Client.class);
    sharedS3Presigner = mock(S3Presigner.class);
    ReflectionTestUtils.setField(factory, "objectStorageS3Client", sharedS3Client);
    ReflectionTestUtils.setField(factory, "objectStorageS3Presigner", sharedS3Presigner);

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
  public void testCreatesS3ClientFromUriWithSharedBean() {
    dataHubConfiguration.getObjectStorage().setUri("s3://my-bucket/prefix");

    ObjectStorageClient client = factory.getInstance();
    assertNotNull(client);
    assertTrue(client instanceof S3ObjectStorageClient);
    assertTrue(client.provider() == ObjectStorageProvider.S3);
  }

  @Test
  public void testClientForReusesSharedS3ClientAcrossLocations() {
    DeleteObjectResponse response = mock(DeleteObjectResponse.class);
    SdkHttpResponse httpResponse = mock(SdkHttpResponse.class);
    when(response.sdkHttpResponse()).thenReturn(httpResponse);
    when(httpResponse.isSuccessful()).thenReturn(true);
    when(sharedS3Client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(response);

    ObjectStorageClient first = factory.clientFor(ObjectStorageLocation.parse("s3://bucket-a"));
    ObjectStorageClient second = factory.clientFor(ObjectStorageLocation.parse("s3://bucket-b"));
    assertNotNull(first);
    assertNotNull(second);

    first.deleteObject(new ObjectStorageReference("bucket-a", "first"));
    second.deleteObject(new ObjectStorageReference("bucket-b", "second"));

    ArgumentCaptor<DeleteObjectRequest> requests =
        ArgumentCaptor.forClass(DeleteObjectRequest.class);
    verify(sharedS3Client, times(2)).deleteObject(requests.capture());
    assertEquals(requests.getAllValues().get(0).bucket(), "bucket-a");
    assertEquals(requests.getAllValues().get(1).bucket(), "bucket-b");
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
  public void testReturnsNullWhenS3LocationWithoutSharedClient() {
    ReflectionTestUtils.setField(factory, "objectStorageS3Client", null);
    ReflectionTestUtils.setField(factory, "objectStorageS3Presigner", null);
    dataHubConfiguration.getObjectStorage().setUri("s3://my-bucket");

    ObjectStorageClient client = factory.getInstance();
    assertNull(client);
  }

  @Test
  public void testReturnsNullWhenS3LocationWithoutSharedPresigner() {
    ReflectionTestUtils.setField(factory, "objectStorageS3Presigner", null);
    dataHubConfiguration.getObjectStorage().setUri("s3://my-bucket");

    ObjectStorageClient client = factory.getInstance();
    assertNull(client);
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
