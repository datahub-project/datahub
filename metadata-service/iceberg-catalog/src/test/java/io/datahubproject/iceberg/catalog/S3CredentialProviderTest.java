package io.datahubproject.iceberg.catalog;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.S3CredentialProvider;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class S3CredentialProviderTest {

  private CredentialProvider.StorageProviderCredentials storageProviderCreds;
  private CredentialProvider.CredentialsCacheKey cacheKey;

  @Mock private StsClient stsClient;

  private S3CredentialProvider credentialProvider;

  @Mock private StsClientBuilder stsClientBuilder;

  @BeforeMethod
  public void setUp() {

    MockitoAnnotations.openMocks(this);

    storageProviderCreds =
        new CredentialProvider.StorageProviderCredentials(
            "testClientId",
            "testClientSecret",
            "arn:aws:iam::123456789012:role/test-role",
            "us-east-1",
            null);

    cacheKey =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform",
            PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
            Set.of("s3://test-bucket/path/to/data"));

    credentialProvider = new S3CredentialProvider();
  }

  @Test
  public void testGetCredentials() {
    StsClientBuilder builderMock = mock(StsClientBuilder.class);
    StsClient clientMock = mock(StsClient.class);

    // Mock the builder chain
    when(builderMock.region(any(Region.class))).thenReturn(builderMock);
    when(builderMock.credentialsProvider(any(StaticCredentialsProvider.class)))
        .thenReturn(builderMock);
    when(builderMock.build()).thenReturn(clientMock);

    AssumeRoleResponse assumeRoleResponse =
        AssumeRoleResponse.builder()
            .credentials(
                Credentials.builder()
                    .accessKeyId("testAccessId-temp")
                    .secretAccessKey("testSecretKey-temp")
                    .sessionToken("testSessionToken-temp")
                    .expiration(java.time.Instant.now().plusSeconds(900))
                    .build())
            .build();
    when(clientMock.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse);

    try (MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
      // Mock the static builder() method
      stsClientMockedStatic.when(StsClient::builder).thenReturn(builderMock);

      /*        "client.region",
       REGION.id(),
       "s3.access-key-id",
       response.credentials().accessKeyId(),
       "s3.secret-access-key",
       response.credentials().secretAccessKey(),
       "s3.session-token",
       response.credentials().sessionToken());

      */

      Map<String, String> creds = credentialProvider.getCredentials(cacheKey, storageProviderCreds);
      assertNotNull(creds);
      assertEquals(creds.get("client.region"), Region.of("us-east-1").id());
      assertEquals(creds.get("s3.access-key-id"), "testAccessId-temp");
      assertEquals(creds.get("s3.secret-access-key"), "testSecretKey-temp");
      assertEquals(creds.get("s3.session-token"), "testSessionToken-temp");
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetWithUnsupportedPrivilege() {
    S3CredentialProvider provider = new S3CredentialProvider();
    CredentialProvider.CredentialsCacheKey keyWithUnsupportedPrivilege =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform",
            PoliciesConfig.DATA_MANAGE_TABLES_PRIVILEGE,
            Set.of("s3://test-bucket/path/to/data"));

    provider.getCredentials(keyWithUnsupportedPrivilege, storageProviderCreds);
  }

  @Test(expectedExceptions = BadRequestException.class)
  public void testGetWithEmptyLocations() {
    S3CredentialProvider provider = new S3CredentialProvider();
    CredentialProvider.CredentialsCacheKey keyWithEmptyLocations =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform", PoliciesConfig.DATA_READ_ONLY_PRIVILEGE, Set.of());

    provider.getCredentials(keyWithEmptyLocations, storageProviderCreds);
  }
}
