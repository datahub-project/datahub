package io.datahubproject.iceberg.catalog.credentials;

import static com.linkedin.metadata.authorization.PoliciesConfig.*;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import org.apache.iceberg.exceptions.BadRequestException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

public class S3CredentialProvider implements CredentialProvider {
  private static final int DEFAULT_CREDS_DURATION_SECS = 60 * 60;

  public Map<String, String> getCredentials(
      CredentialsCacheKey key, StorageProviderCredentials storageProviderCredentials) {

    int expiration =
        storageProviderCredentials.tempCredentialExpirationSeconds == null
            ? DEFAULT_CREDS_DURATION_SECS
            : storageProviderCredentials.tempCredentialExpirationSeconds;
    try (StsClient stsClient = stsClient(storageProviderCredentials)) {
      String sessionPolicy = policyString(key);
      AssumeRoleResponse response =
          stsClient.assumeRole(
              AssumeRoleRequest.builder()
                  .roleArn(storageProviderCredentials.role)
                  .roleSessionName("DataHubIcebergSession")
                  .durationSeconds(expiration)
                  .policy(sessionPolicy)
                  .build());

      return Map.of(
          "client.region",
          storageProviderCredentials.region,
          "s3.access-key-id",
          response.credentials().accessKeyId(),
          "s3.secret-access-key",
          response.credentials().secretAccessKey(),
          "s3.session-token",
          response.credentials().sessionToken());
    }
  }

  private StsClient stsClient(StorageProviderCredentials storageProviderCredentials) {
    AwsBasicCredentials credentials =
        AwsBasicCredentials.create(
            storageProviderCredentials.clientId, storageProviderCredentials.clientSecret);
    return StsClient.builder()
        .region(Region.of(storageProviderCredentials.region))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .region(Region.of(storageProviderCredentials.region))
        .build();
  }

  private String policyString(CredentialsCacheKey key) {
    if (key.locations == null || key.locations.isEmpty()) {
      throw new BadRequestException("Unspecified locations for credential vending.");
    }
    if (!Set.of(DATA_READ_WRITE_PRIVILEGE, DATA_READ_ONLY_PRIVILEGE).contains(key.privilege)) {
      throw new IllegalStateException("Unsupported credential vending privilege " + key.privilege);
    }

    Map<String, IamStatement.Builder> bucketListPolicy = new HashMap<>();
    IamStatement.Builder objectsPolicy =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");

    if (DATA_READ_WRITE_PRIVILEGE.equals(key.privilege)) {
      objectsPolicy.addAction("s3:PutObject").addAction("s3:DeleteObject");
    }

    key.locations.forEach(
        location -> {
          S3Location s3Location = new S3Location(location);
          objectsPolicy.addResource(s3Location.objectsArn());
          bucketListPolicy
              .computeIfAbsent(
                  s3Location.bucketArn(),
                  bucketArn ->
                      IamStatement.builder()
                          .effect(IamEffect.ALLOW)
                          .addAction("s3:ListBucket")
                          .addResource(bucketArn))
              .addCondition(
                  IamConditionOperator.STRING_LIKE, "s3:prefix", s3Location.objectsPathPrefix());
        });

    IamPolicy.Builder sessionPolicyBuilder = IamPolicy.builder();
    sessionPolicyBuilder.addStatement(objectsPolicy.build());

    for (Map.Entry<String, IamStatement.Builder> bucketListStatement :
        bucketListPolicy.entrySet()) {
      sessionPolicyBuilder.addStatement(bucketListStatement.getValue().build());

      String bucketArn = bucketListStatement.getKey();
      sessionPolicyBuilder.addStatement(
          IamStatement.builder()
              .effect(IamEffect.ALLOW)
              .addAction("s3:GetBucketLocation")
              .addResource(bucketArn)
              .build());
    }
    return sessionPolicyBuilder.build().toJson();
  }

  @EqualsAndHashCode
  private static class S3Location {
    private final String bucket;
    private final String path;
    private final String s3ArnPrefix;

    S3Location(String location) {
      URI uri = URI.create(location);
      this.bucket = uri.getAuthority();
      String path = uri.getPath();
      if (path.startsWith("/")) {
        path = path.substring(1);
      }
      this.path = path;
      this.s3ArnPrefix = "arn:aws:s3:::";
    }

    String objectsArn() {
      return bucketArn() + "/" + objectsPathPrefix();
    }

    String bucketArn() {
      return s3ArnPrefix + bucket;
    }

    String objectsPathPrefix() {
      return path + "/*";
    }
  }
}
