package io.datahubproject.iceberg.catalog;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.S3CredentialProvider;
import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.testng.SkipException;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class S3CredentialProviderHistogramTest {

  private static final String WEB_IDENTITY_PROVIDER_CLASS =
      "software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider";

  @Test(timeOut = 60_000)
  public void repeatedVendingWithSharedStsClientDoesNotGrowWebIdentityProviders() throws Exception {
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

    int before = countLiveWebIdentityStsProviders();
    try (S3CredentialProvider provider = new S3CredentialProvider(stsClient)) {
      for (int i = 0; i < 16; i++) {
        provider.getCredentials(key, creds);
      }
    }
    int after = countLiveWebIdentityStsProviders();
    assertTrue(
        after - before <= 1,
        "injected StsClient vending must not grow live "
            + WEB_IDENTITY_PROVIDER_CLASS
            + " (before="
            + before
            + ", after="
            + after
            + ")");
    verify(stsClient, never()).close();
  }

  private static int countLiveWebIdentityStsProviders() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName diagnostic = new ObjectName("com.sun.management:type=DiagnosticCommand");
    String histogram;
    try {
      histogram =
          (String)
              server.invoke(
                  diagnostic,
                  "gcClassHistogram",
                  new Object[] {new String[0]},
                  new String[] {"[Ljava.lang.String;"});
    } catch (Exception e) {
      throw new SkipException("gcClassHistogram is unavailable: " + e.getMessage());
    }
    int total = 0;
    for (String line : histogram.split("\n")) {
      String trimmed = line.trim();
      if (!trimmed.endsWith(WEB_IDENTITY_PROVIDER_CLASS)) {
        continue;
      }
      String[] columns = trimmed.split("\\s+");
      if (columns.length < 2) {
        continue;
      }
      total += Integer.parseInt(columns[1]);
    }
    return total;
  }
}
