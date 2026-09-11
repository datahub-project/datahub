package com.linkedin.gms.factory.aws;

import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import java.util.Properties;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class AwsJdbcIamAuthTest {

  @AfterMethod
  public void resetHandler() {
    AwsJdbcIamAuth.reset();
  }

  @Test
  public void installBindsTheSameProviderOnEveryLookup() {
    AwsCredentialsProvider shared =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("id", "secret"));
    AwsJdbcIamAuth.installSharedCredentials(shared);

    HostSpec host = hostSpec();
    Properties properties = new Properties();
    assertSame(AwsCredentialsManager.getProvider(host, properties), shared);
    assertSame(AwsCredentialsManager.getProvider(host, properties), shared);
  }

  @Test
  public void installNullDoesNotReplaceAnExistingHandler() {
    AwsCredentialsProvider shared =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("id", "secret"));
    AwsJdbcIamAuth.installSharedCredentials(shared);
    AwsJdbcIamAuth.installSharedCredentials(null);

    assertSame(AwsCredentialsManager.getProvider(hostSpec(), new Properties()), shared);
  }

  @Test
  public void resetRestoresPerCallDefaultChainConstruction() {
    AwsCredentialsProvider shared =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("id", "secret"));
    AwsJdbcIamAuth.installSharedCredentials(shared);
    AwsJdbcIamAuth.reset();

    HostSpec host = hostSpec();
    Properties properties = new Properties();
    AwsCredentialsProvider first = AwsCredentialsManager.getProvider(host, properties);
    AwsCredentialsProvider second = AwsCredentialsManager.getProvider(host, properties);
    assertNotSame(first, second);
    assertNotSame(first, shared);
  }

  private static HostSpec hostSpec() {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("localhost")
        .port(3306)
        .build();
  }
}
