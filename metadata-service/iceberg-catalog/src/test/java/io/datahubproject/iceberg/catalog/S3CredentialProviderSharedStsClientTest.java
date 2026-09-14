package io.datahubproject.iceberg.catalog;

import static org.mockito.Mockito.*;

import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.S3CredentialProvider;
import java.util.Set;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class S3CredentialProviderSharedStsClientTest {

  @Test
  public void repeatedVendingWithSharedStsClientDoesNotCloseInjectedClient() {
    StsClient stsClient = mock(StsClient.class);
    when(stsClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId("ak")
                        .secretAccessKey("sk")
                        .sessionToken("st")
                        .expiration(java.time.Instant.now().plusSeconds(900))
                        .build())
                .build());

    CredentialProvider.StorageProviderCredentials creds =
        new CredentialProvider.StorageProviderCredentials(
            null, null, "arn:aws:iam::123456789012:role/test-role", "us-east-1", null);
    CredentialProvider.CredentialsCacheKey key =
        new CredentialProvider.CredentialsCacheKey(
            "testPlatform",
            PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
            Set.of("s3://test-bucket/path"));

    try (S3CredentialProvider provider = new S3CredentialProvider(stsClient)) {
      for (int i = 0; i < 16; i++) {
        provider.getCredentials(key, creds);
      }
    }
    verify(stsClient, times(16)).assumeRole(any(AssumeRoleRequest.class));
    verify(stsClient, never()).close();
  }
}
