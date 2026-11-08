package io.datahubproject.iceberg.catalog;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

public class VendedCredentialsS3FileIOTest {

  private static Map<String, String> vendedCreds() {
    return Map.of(
        VendedCredentialsS3FileIO.ACCESS_KEY_ID,
        "AKIATEST",
        VendedCredentialsS3FileIO.SECRET_ACCESS_KEY,
        "secret",
        VendedCredentialsS3FileIO.SESSION_TOKEN,
        "session",
        VendedCredentialsS3FileIO.CLIENT_REGION,
        "us-east-1");
  }

  @Test
  public void buildsClientWithStaticCredentialsNotDefaultChain() {
    S3Client client = VendedCredentialsS3FileIO.buildS3Client(vendedCreds());
    try {
      assertTrue(
          client.serviceClientConfiguration().credentialsProvider()
              instanceof StaticCredentialsProvider);
      assertEquals(client.serviceClientConfiguration().region().id(), "us-east-1");
    } finally {
      client.close();
    }
  }

  @Test
  public void createAndCloseDoesNotRequireDefaultChain() throws Exception {
    FileIO io = VendedCredentialsS3FileIO.create(vendedCreds());
    io.close();
  }

  @Test
  public void closingFileIOEqualsItself() throws Exception {
    FileIO io = VendedCredentialsS3FileIO.create(vendedCreds());
    try {
      assertTrue(io.equals(io));
    } finally {
      io.close();
    }
  }

  @Test
  public void repeatedCreateCloseDoesNotFallBackToDefaultChain() throws Exception {
    for (int i = 0; i < 8; i++) {
      FileIO io = VendedCredentialsS3FileIO.create(vendedCreds());
      io.close();
    }
  }

  @Test
  public void catalogCacheReusesOneClientForSameVendedKeys() throws Exception {
    VendedCredentialsS3FileIO.VendedS3ClientCache cache =
        new VendedCredentialsS3FileIO.VendedS3ClientCache();
    try {
      FileIO first = VendedCredentialsS3FileIO.create(vendedCreds(), cache);
      FileIO second = VendedCredentialsS3FileIO.create(vendedCreds(), cache);
      assertEquals(cache.size(), 1);
      first.close();
      second.close();
      assertEquals(cache.size(), 1);
    } finally {
      cache.close();
    }
  }

  @Test
  public void catalogCacheKeepsSeparateClientsWhenSessionTokenChanges() throws Exception {
    VendedCredentialsS3FileIO.VendedS3ClientCache cache =
        new VendedCredentialsS3FileIO.VendedS3ClientCache();
    try {
      Map<String, String> first = vendedCreds();
      Map<String, String> rotated = new HashMap<>(first);
      rotated.put(VendedCredentialsS3FileIO.SESSION_TOKEN, "rotated");
      FileIO a = VendedCredentialsS3FileIO.create(first, cache);
      FileIO b = VendedCredentialsS3FileIO.create(rotated, cache);
      assertEquals(cache.size(), 2);
      a.close();
      b.close();
    } finally {
      cache.close();
    }
  }

  @Test
  public void missingAccessKeysRefuseDefaultChain() {
    assertThrows(
        IllegalStateException.class,
        () -> VendedCredentialsS3FileIO.buildS3Client(Map.of("client.region", "us-east-1")));
  }
}
