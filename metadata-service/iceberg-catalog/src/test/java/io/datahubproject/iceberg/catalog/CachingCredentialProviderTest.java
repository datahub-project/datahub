package io.datahubproject.iceberg.catalog;

import static org.testng.Assert.*;

import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.iceberg.catalog.credentials.CachingCredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.S3CredentialProvider;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

public class CachingCredentialProviderTest {

  private CredentialProvider.StorageProviderCredentials storageProviderCreds;
  private CredentialProvider.CredentialsCacheKey cacheKey;

  @Mock private StsClient stsClient;

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
  }

  @Test
  public void testCredentialCaching() {
    TestableS3CredentialProvider backingProvider = new TestableS3CredentialProvider();
    CachingCredentialProvider provider = new CachingCredentialProvider(backingProvider);

    Map<String, String> firstResult = provider.getCredentials(cacheKey, storageProviderCreds);
    int firstLoadCount = backingProvider.getLoadCount();

    Map<String, String> secondResult = provider.getCredentials(cacheKey, storageProviderCreds);
    int secondLoadCount = backingProvider.getLoadCount();

    assertEquals(firstLoadCount, 1, "First call should load credentials");
    assertEquals(secondLoadCount, 1, "Second call should use cached credentials");
    assertSame(firstResult, secondResult, "Should return same cached credential map");
  }

  @Test
  public void testDifferentKeysGetDifferentCredentials() {
    TestableS3CredentialProvider backingProvider = new TestableS3CredentialProvider();
    CachingCredentialProvider provider = new CachingCredentialProvider(backingProvider);

    CredentialProvider.CredentialsCacheKey secondKey =
        new CredentialProvider.CredentialsCacheKey(
            "differentPlatform",
            PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
            Set.of("s3://test-bucket/path/to/data"));

    Map<String, String> firstResult = provider.getCredentials(cacheKey, storageProviderCreds);
    int firstLoadCount = backingProvider.getLoadCount();

    Map<String, String> secondResult = provider.getCredentials(secondKey, storageProviderCreds);
    int secondLoadCount = backingProvider.getLoadCount();

    assertEquals(firstLoadCount, 1, "First call should load credentials");
    assertEquals(secondLoadCount, 2, "Different key should trigger new credential load");
    assertNotSame(firstResult, secondResult, "Different keys should get different credential maps");
  }

  /**
   * Test implementation that allows us to verify the number of credential loads without relying on
   * implementation details.
   */
  private static class TestableS3CredentialProvider extends S3CredentialProvider {
    private int loadCount = 0;

    @Override
    public Map<String, String> getCredentials(
        CredentialsCacheKey key, StorageProviderCredentials storageProviderCredentials) {
      loadCount++;
      return Map.of(
          "client.region", storageProviderCredentials.region,
          "s3.access-key-id", "TESTACCESSKEY" + loadCount, // Make each load unique
          "s3.secret-access-key", "TESTSECRETKEY" + loadCount,
          "s3.session-token", "TESTSESSIONTOKEN" + loadCount);
    }

    public int getLoadCount() {
      return loadCount;
    }
  }
}
