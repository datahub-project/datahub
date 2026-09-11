package com.linkedin.gms.factory.aws;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import java.util.Properties;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class AwsClientFactoryShutdownTest {

  @Mock private ConfigurationProvider configurationProvider;

  private AwsClientFactory awsClientFactory;
  private AutoCloseable mocks;

  @BeforeMethod
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    awsClientFactory = new AwsClientFactory();
    ReflectionTestUtils.setField(awsClientFactory, "configurationProvider", configurationProvider);

    DataHubConfiguration dataHubConfiguration = new DataHubConfiguration();
    dataHubConfiguration.setObjectStorage(new ObjectStorageConfiguration());
    org.mockito.Mockito.when(configurationProvider.getDatahub()).thenReturn(dataHubConfiguration);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    AwsJdbcIamAuth.reset();
    System.clearProperty("aws.region");
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void shutdownClosesManagedS3ClientAndPresigner() {
    S3Client s3Client = mock(S3Client.class);
    S3Presigner presigner = mock(S3Presigner.class);
    ReflectionTestUtils.setField(awsClientFactory, "managedObjectStorageS3Client", s3Client);
    ReflectionTestUtils.setField(awsClientFactory, "managedObjectStorageS3Presigner", presigner);

    awsClientFactory.shutdown();

    verify(presigner).close();
    verify(s3Client).close();
  }

  @Test
  public void shutdownClosesObjectStorageRoleProvider() {
    S3Client s3Client = mock(S3Client.class);
    StsAssumeRoleCredentialsProvider roleProvider = mock(StsAssumeRoleCredentialsProvider.class);
    ReflectionTestUtils.setField(awsClientFactory, "managedObjectStorageS3Client", s3Client);
    ReflectionTestUtils.setField(
        awsClientFactory, "objectStorageRoleCredentialsProvider", roleProvider);

    awsClientFactory.shutdown();

    verify(roleProvider).close();
    verify(s3Client).close();
  }

  @Test
  public void shutdownClosesDefaultCredentialsProvider() {
    DefaultCredentialsProvider credentialsProvider = mock(DefaultCredentialsProvider.class);
    ReflectionTestUtils.setField(
        awsClientFactory, "defaultCredentialsProvider", credentialsProvider);

    awsClientFactory.shutdown();

    verify(credentialsProvider).close();
  }

  @Test
  public void defaultCredentialsProviderInstallsJdbcIamHandler() {
    System.setProperty("aws.region", "us-east-1");
    AwsCredentialsProvider shared = awsClientFactory.defaultAwsCredentialsProvider();
    assertNotNull(shared);
    HostSpec host =
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("localhost")
            .port(3306)
            .build();
    assertSame(AwsCredentialsManager.getProvider(host, new Properties()), shared);
    awsClientFactory.shutdown();
  }
}
