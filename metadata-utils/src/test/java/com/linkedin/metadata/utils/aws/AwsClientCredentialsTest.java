package com.linkedin.metadata.utils.aws;

import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.Optional;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class AwsClientCredentialsTest {

  @Test
  public void requireFromReadsExplicitS3ClientCredentials() {
    StaticCredentialsProvider provider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "secret"));
    try (S3Client s3Client =
        S3Client.builder().region(Region.US_EAST_1).credentialsProvider(provider).build()) {
      assertTrue(AwsClientCredentials.requireFrom(s3Client) instanceof StaticCredentialsProvider);
    }
  }

  @Test
  public void requireRejectsNullAndEmptyOptional() {
    assertThrows(IllegalStateException.class, () -> AwsClientCredentials.require(null));
    assertThrows(IllegalStateException.class, () -> AwsClientCredentials.require(Optional.empty()));
  }

  @Test
  public void requireUnwrapsOptionalProvider() {
    StaticCredentialsProvider provider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "secret"));
    assertSame(provider, AwsClientCredentials.require(Optional.of(provider)));
  }
}
