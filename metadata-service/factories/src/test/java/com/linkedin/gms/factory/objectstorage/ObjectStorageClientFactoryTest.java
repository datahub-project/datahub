package com.linkedin.gms.factory.objectstorage;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import com.linkedin.metadata.config.S3Configuration;
import com.linkedin.metadata.utils.objectstorage.LocalObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageProvider;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
    S3Configuration s3Configuration = new S3Configuration();
    s3Configuration.setBucketName("");
    dataHubConfiguration.setS3(s3Configuration);

    ObjectStorageConfiguration objectStorageConfiguration = new ObjectStorageConfiguration();
    dataHubConfiguration.setObjectStorage(objectStorageConfiguration);

    when(configurationProvider.getDatahub()).thenReturn(dataHubConfiguration);
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
