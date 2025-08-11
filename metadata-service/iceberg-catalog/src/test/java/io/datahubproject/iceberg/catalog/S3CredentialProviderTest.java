package io.datahubproject.iceberg.catalog;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.S3CredentialProvider;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.mockito.ArgumentCaptor;
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

  @Test
  public void testObjectsPathPrefix_DefaultBehavior() {
    // Test the default behavior when IRC_DISABLE_PATH_PREFIX_POLICY is not set
    // This test verifies that the policy includes the specific path prefix

    CredentialProvider.CredentialsCacheKey testKey =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform",
            PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
            Set.of("s3://test-bucket/path/to/data"));

    // Mock the STS client to capture the policy
    StsClientBuilder builderMock = mock(StsClientBuilder.class);
    StsClient clientMock = mock(StsClient.class);
    when(builderMock.region(any(Region.class))).thenReturn(builderMock);
    when(builderMock.credentialsProvider(any(StaticCredentialsProvider.class)))
        .thenReturn(builderMock);
    when(builderMock.build()).thenReturn(clientMock);

    AssumeRoleResponse assumeRoleResponse =
        AssumeRoleResponse.builder()
            .credentials(
                Credentials.builder()
                    .accessKeyId("testAccessId")
                    .secretAccessKey("testSecretKey")
                    .sessionToken("testSessionToken")
                    .expiration(java.time.Instant.now().plusSeconds(900))
                    .build())
            .build();
    when(clientMock.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse);

    try (MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
      stsClientMockedStatic.when(StsClient::builder).thenReturn(builderMock);

      // Capture the policy argument
      ArgumentCaptor<AssumeRoleRequest> requestCaptor =
          ArgumentCaptor.forClass(AssumeRoleRequest.class);

      credentialProvider.getCredentials(testKey, storageProviderCreds);

      // Verify that assumeRole was called and capture the request
      verify(clientMock).assumeRole(requestCaptor.capture());

      // Get the captured request and verify the policy content
      AssumeRoleRequest capturedRequest = requestCaptor.getValue();
      String policy = capturedRequest.policy();

      // Verify the policy contains the expected path prefix
      assertTrue(policy.contains("s3:prefix"), "Policy should contain s3:prefix condition");
      assertTrue(
          policy.contains("path/to/data/*"), "Policy should contain the specific path prefix");
    }
  }

  @Test
  public void testObjectsPathPrefix_WithDifferentPathFormats() {
    // Test with different S3 path formats to ensure proper path handling

    CredentialProvider.CredentialsCacheKey testKeyWithSlash =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform",
            PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
            Set.of("s3://test-bucket/path/with/slash"));

    // Mock the STS client
    StsClientBuilder builderMock = mock(StsClientBuilder.class);
    StsClient clientMock = mock(StsClient.class);
    when(builderMock.region(any(Region.class))).thenReturn(builderMock);
    when(builderMock.credentialsProvider(any(StaticCredentialsProvider.class)))
        .thenReturn(builderMock);
    when(builderMock.build()).thenReturn(clientMock);

    AssumeRoleResponse assumeRoleResponse =
        AssumeRoleResponse.builder()
            .credentials(
                Credentials.builder()
                    .accessKeyId("testAccessId")
                    .secretAccessKey("testSecretKey")
                    .sessionToken("testSessionToken")
                    .expiration(java.time.Instant.now().plusSeconds(900))
                    .build())
            .build();
    when(clientMock.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse);

    try (MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
      stsClientMockedStatic.when(StsClient::builder).thenReturn(builderMock);

      // Capture the policy argument
      ArgumentCaptor<AssumeRoleRequest> requestCaptor =
          ArgumentCaptor.forClass(AssumeRoleRequest.class);

      credentialProvider.getCredentials(testKeyWithSlash, storageProviderCreds);

      // Verify that assumeRole was called and capture the request
      verify(clientMock).assumeRole(requestCaptor.capture());

      // Get the captured request and verify the policy content
      AssumeRoleRequest capturedRequest = requestCaptor.getValue();
      String policy = capturedRequest.policy();

      // Verify the policy contains the expected path prefix
      assertTrue(policy.contains("s3:prefix"), "Policy should contain s3:prefix condition");
      assertTrue(
          policy.contains("path/with/slash/*"), "Policy should contain the correct path prefix");
    }
  }

  @Test
  public void testObjectsPathPrefix_WithMultipleLocations() {
    // Test with multiple S3 locations to ensure all are included in the policy

    CredentialProvider.CredentialsCacheKey testKeyWithMultipleLocations =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform",
            PoliciesConfig.DATA_READ_WRITE_PRIVILEGE,
            Set.of("s3://bucket1/path1", "s3://bucket2/path2"));

    // Mock the STS client
    StsClientBuilder builderMock = mock(StsClientBuilder.class);
    StsClient clientMock = mock(StsClient.class);
    when(builderMock.region(any(Region.class))).thenReturn(builderMock);
    when(builderMock.credentialsProvider(any(StaticCredentialsProvider.class)))
        .thenReturn(builderMock);
    when(builderMock.build()).thenReturn(clientMock);

    AssumeRoleResponse assumeRoleResponse =
        AssumeRoleResponse.builder()
            .credentials(
                Credentials.builder()
                    .accessKeyId("testAccessId")
                    .secretAccessKey("testSecretKey")
                    .sessionToken("testSessionToken")
                    .expiration(java.time.Instant.now().plusSeconds(900))
                    .build())
            .build();
    when(clientMock.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse);

    try (MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
      stsClientMockedStatic.when(StsClient::builder).thenReturn(builderMock);

      // Capture the policy argument
      ArgumentCaptor<AssumeRoleRequest> requestCaptor =
          ArgumentCaptor.forClass(AssumeRoleRequest.class);

      credentialProvider.getCredentials(testKeyWithMultipleLocations, storageProviderCreds);

      // Verify that assumeRole was called and capture the request
      verify(clientMock).assumeRole(requestCaptor.capture());

      // Get the captured request and verify the policy content
      AssumeRoleRequest capturedRequest = requestCaptor.getValue();
      String policy = capturedRequest.policy();

      // Verify the policy contains both path prefixes
      assertTrue(policy.contains("s3:prefix"), "Policy should contain s3:prefix condition");
      assertTrue(policy.contains("path1/*"), "Policy should contain path1 prefix");
      assertTrue(policy.contains("path2/*"), "Policy should contain path2 prefix");

      // Verify that write permissions are included for DATA_READ_WRITE_PRIVILEGE
      assertTrue(
          policy.contains("s3:PutObject"),
          "Policy should contain PutObject for read-write privilege");
      assertTrue(
          policy.contains("s3:DeleteObject"),
          "Policy should contain DeleteObject for read-write privilege");
    }
  }

  @Test
  public void testObjectsPathPrefix_WithEnvironmentVariableSet() {
    // Test when IRC_DISABLE_PATH_PREFIX_POLICY is set to "true"
    // This test requires the environment variable to be set before running

    // Check if the environment variable is set to true
    String disablePathPrefixPolicy = System.getenv("IRC_DISABLE_PATH_PREFIX_POLICY");
    boolean shouldTestWildcard = "true".equalsIgnoreCase(disablePathPrefixPolicy);

    CredentialProvider.CredentialsCacheKey testKey =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform",
            PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
            Set.of("s3://test-bucket/path/to/data"));

    // Mock the STS client
    StsClientBuilder builderMock = mock(StsClientBuilder.class);
    StsClient clientMock = mock(StsClient.class);
    when(builderMock.region(any(Region.class))).thenReturn(builderMock);
    when(builderMock.credentialsProvider(any(StaticCredentialsProvider.class)))
        .thenReturn(builderMock);
    when(builderMock.build()).thenReturn(clientMock);

    AssumeRoleResponse assumeRoleResponse =
        AssumeRoleResponse.builder()
            .credentials(
                Credentials.builder()
                    .accessKeyId("testAccessId")
                    .secretAccessKey("testSecretKey")
                    .sessionToken("testSessionToken")
                    .expiration(java.time.Instant.now().plusSeconds(900))
                    .build())
            .build();
    when(clientMock.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse);

    try (MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
      stsClientMockedStatic.when(StsClient::builder).thenReturn(builderMock);

      // Capture the policy argument
      ArgumentCaptor<AssumeRoleRequest> requestCaptor =
          ArgumentCaptor.forClass(AssumeRoleRequest.class);

      credentialProvider.getCredentials(testKey, storageProviderCreds);

      // Verify that assumeRole was called and capture the request
      verify(clientMock).assumeRole(requestCaptor.capture());

      // Get the captured request and verify the policy content
      AssumeRoleRequest capturedRequest = requestCaptor.getValue();
      String policy = capturedRequest.policy();

      // Verify the policy contains s3:prefix condition
      assertTrue(policy.contains("s3:prefix"), "Policy should contain s3:prefix condition");

      if (shouldTestWildcard) {
        // If environment variable is set to true, verify wildcard path prefix
        assertTrue(
            policy.contains("\"*\""),
            "Policy should contain wildcard path prefix when IRC_DISABLE_PATH_PREFIX_POLICY=true");
      } else {
        // If environment variable is not set or not true, verify specific path prefix
        assertTrue(
            policy.contains("path/to/data/*"),
            "Policy should contain the specific path prefix when IRC_DISABLE_PATH_PREFIX_POLICY is not set to true");
      }
    }
  }
}
