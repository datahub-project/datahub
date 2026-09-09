package com.linkedin.gms.factory.aws;

import static org.testng.Assert.assertNull;

import com.linkedin.gms.factory.s3.StsClientFactory;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Under fake IRSA system properties, GMS factories must skip client construction (no implicit
 * default chain) rather than allocate {@code StsAssumeRoleWithWebIdentityCredentialsProvider}.
 */
public class AwsIrsaCredentialProviderHistogramTest {

  private static final String WEB_IDENTITY_PROVIDER_CLASS =
      "software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider";

  private static final String TEST_ROLE_ARN =
      "arn:aws:iam::123456789012:role/datahub-histogram-test";

  @AfterMethod
  public void clearAwsProperties() {
    System.clearProperty("AWS_REGION");
    System.clearProperty("aws.region");
    System.clearProperty("AWS_ENDPOINT_URL");
    System.clearProperty("aws.roleArn");
    System.clearProperty("aws.webIdentityTokenFile");
  }

  @Test(timeOut = 60_000)
  public void factoriesDoNotGrowLiveWebIdentityStsProvidersUnderFakeIrsa() throws Exception {
    Path tokenFile = Files.createTempFile("datahub-irsa", ".token");
    Files.writeString(tokenFile, "e30.e30.e30");
    System.setProperty("aws.region", "us-east-1");
    System.setProperty("aws.roleArn", TEST_ROLE_ARN);
    System.setProperty("aws.webIdentityTokenFile", tokenFile.toAbsolutePath().toString());

    try {
      // Histogram is only used to confirm this JVM can observe the leak class; skip otherwise.
      if (measureExplicitWebIdentityProviderGrowth() <= 0) {
        throw new SkipException(
            "gcClassHistogram did not observe live "
                + WEB_IDENTITY_PROVIDER_CLASS
                + " instances; histogram assertion is not meaningful");
      }

      for (int i = 0; i < 8; i++) {
        assertNull(new StsClientFactoryProbe().getInstance());
        AwsClientFactory factory = new AwsClientFactory();
        assertNull(factory.objectStorageS3Client(null));
        factory.shutdown();
      }
    } finally {
      Files.deleteIfExists(tokenFile);
    }
  }

  /**
   * Confirms the histogram can see the leak class, then closes the probe providers and their dummy
   * STS clients.
   */
  private static int measureExplicitWebIdentityProviderGrowth() throws Exception {
    int before = countLiveWebIdentityStsProviders();
    List<SdkAutoCloseable> owned = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        StsClient dummySts =
            StsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .endpointOverride(URI.create("http://127.0.0.1:1"))
                .build();
        owned.add(dummySts);
        owned.add(
            StsAssumeRoleWithWebIdentityCredentialsProvider.builder()
                .stsClient(dummySts)
                .asyncCredentialUpdateEnabled(false)
                .refreshRequest(
                    request -> request.roleArn(TEST_ROLE_ARN).webIdentityToken("e30.e30.e30"))
                .build());
      }
      return countLiveWebIdentityStsProviders() - before;
    } finally {
      for (int i = owned.size() - 1; i >= 0; i--) {
        owned.get(i).close();
      }
    }
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

  /** Subclass so this test package can call the protected factory method. */
  private static final class StsClientFactoryProbe extends StsClientFactory {
    @Override
    protected StsClient getInstance() {
      return super.getInstance();
    }
  }
}
