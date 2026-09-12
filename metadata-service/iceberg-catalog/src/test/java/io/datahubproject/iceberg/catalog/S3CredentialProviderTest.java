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

  @BeforeMethod
  public void setUp() {

    MockitoAnnotations.openMocks(this);

    storageProviderCreds =
        new CredentialProvider.StorageProviderCredentials(
            null, null, "arn:aws:iam::123456789012:role/test-role", "us-east-1", null);

    cacheKey =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform",
            PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
            Set.of("s3://test-bucket/path/to/data"));

    credentialProvider = new S3CredentialProvider(stsClient);
  }

  private void stubAssumeRole() {
    stubAssumeRole(stsClient);
  }

  private static void stubAssumeRole(StsClient client) {
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
    when(client.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse);
  }

  @Test
  public void testGetCredentials() {
    stubAssumeRole();

    Map<String, String> creds = credentialProvider.getCredentials(cacheKey, storageProviderCreds);
    assertNotNull(creds);
    assertEquals(creds.get("client.region"), "us-east-1");
    assertEquals(creds.get("s3.access-key-id"), "testAccessId-temp");
    assertEquals(creds.get("s3.secret-access-key"), "testSecretKey-temp");
    assertEquals(creds.get("s3.session-token"), "testSessionToken-temp");
    credentialProvider.close();
    verify(stsClient, never()).close();
  }

  @Test
  public void testGetCredentialsReusesInjectedStsClient() {
    stubAssumeRole();
    credentialProvider.getCredentials(cacheKey, storageProviderCreds);
    credentialProvider.getCredentials(cacheKey, storageProviderCreds);
    credentialProvider.close();
    verify(stsClient, times(2)).assumeRole(any(AssumeRoleRequest.class));
    verify(stsClient, never()).close();
  }

  @Test
  public void warehouseStaticKeysReuseAndCloseOwnedStsClient() {
    StsClient warehouseClient = mock(StsClient.class);
    stubAssumeRole(warehouseClient);
    StsClientBuilder builder = mock(StsClientBuilder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(warehouseClient);

    CredentialProvider.StorageProviderCredentials keyed =
        new CredentialProvider.StorageProviderCredentials(
            "client-id",
            "client-secret",
            "arn:aws:iam::123456789012:role/test-role",
            "us-east-1",
            null);

    try (MockedStatic<StsClient> stsStatic = mockStatic(StsClient.class)) {
      stsStatic.when(StsClient::builder).thenReturn(builder);
      try (S3CredentialProvider provider = new S3CredentialProvider()) {
        provider.getCredentials(cacheKey, keyed);
        provider.getCredentials(cacheKey, keyed);
      }
    }

    verify(builder, times(1)).build();
    verify(warehouseClient, times(2)).assumeRole(any(AssumeRoleRequest.class));
    verify(warehouseClient).close();
  }

  @Test
  public void warehouseStaticKeysDifferentSecretsDoNotShareStsClient() {
    StsClient firstClient = mock(StsClient.class);
    StsClient secondClient = mock(StsClient.class);
    stubAssumeRole(firstClient);
    stubAssumeRole(secondClient);
    StsClientBuilder builder = mock(StsClientBuilder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(firstClient, secondClient);

    CredentialProvider.StorageProviderCredentials firstKeys =
        new CredentialProvider.StorageProviderCredentials(
            "client-id", "secret-a", "arn:aws:iam::123456789012:role/test-role", "us-east-1", null);
    CredentialProvider.StorageProviderCredentials rotatedKeys =
        new CredentialProvider.StorageProviderCredentials(
            "client-id", "secret-b", "arn:aws:iam::123456789012:role/test-role", "us-east-1", null);

    try (MockedStatic<StsClient> stsStatic = mockStatic(StsClient.class)) {
      stsStatic.when(StsClient::builder).thenReturn(builder);
      try (S3CredentialProvider provider = new S3CredentialProvider()) {
        provider.getCredentials(cacheKey, firstKeys);
        provider.getCredentials(cacheKey, rotatedKeys);
        verify(firstClient).close();
      }
    }

    verify(builder, times(2)).build();
    verify(secondClient).close();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetCredentialsRequiresKeysOrSharedClient() {
    S3CredentialProvider provider = new S3CredentialProvider();
    provider.getCredentials(cacheKey, storageProviderCreds);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetWithUnsupportedPrivilege() {
    S3CredentialProvider provider = new S3CredentialProvider(stsClient);
    CredentialProvider.CredentialsCacheKey keyWithUnsupportedPrivilege =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform",
            PoliciesConfig.DATA_MANAGE_TABLES_PRIVILEGE,
            Set.of("s3://test-bucket/path/to/data"));

    provider.getCredentials(keyWithUnsupportedPrivilege, storageProviderCreds);
  }

  @Test(expectedExceptions = BadRequestException.class)
  public void testGetWithEmptyLocations() {
    S3CredentialProvider provider = new S3CredentialProvider(stsClient);
    CredentialProvider.CredentialsCacheKey keyWithEmptyLocations =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform", PoliciesConfig.DATA_READ_ONLY_PRIVILEGE, Set.of());

    provider.getCredentials(keyWithEmptyLocations, storageProviderCreds);
  }
}
