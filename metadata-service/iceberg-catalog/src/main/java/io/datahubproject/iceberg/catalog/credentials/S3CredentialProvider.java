package io.datahubproject.iceberg.catalog.credentials;

import static com.linkedin.metadata.authorization.PoliciesConfig.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
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

public class S3CredentialProvider implements CredentialProvider, AutoCloseable {
  private static final int DEFAULT_CREDS_DURATION_SECS = 60 * 60;

  @Nullable private final StsClient injectedStsClient;
  private final ConcurrentHashMap<String, OwnedStsClient> ownedClients = new ConcurrentHashMap<>();
  private final Object lifecycle = new Object();
  private volatile boolean closed;

  public S3CredentialProvider() {
    this(null);
  }

  public S3CredentialProvider(@Nullable StsClient stsClient) {
    this.injectedStsClient = stsClient;
  }

  public Map<String, String> getCredentials(
      CredentialsCacheKey key, StorageProviderCredentials storageProviderCredentials) {

    int expiration =
        storageProviderCredentials.tempCredentialExpirationSeconds == null
            ? DEFAULT_CREDS_DURATION_SECS
            : storageProviderCredentials.tempCredentialExpirationSeconds;
    String sessionPolicy = policyString(key);
    StsClient client = null;
    boolean leasedWarehouseClient = false;
    try {
      synchronized (lifecycle) {
        client = acquireStsClientLocked(storageProviderCredentials);
        leasedWarehouseClient = hasStaticKeys(storageProviderCredentials);
      }
      AssumeRoleResponse response =
          client.assumeRole(
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
    } finally {
      if (leasedWarehouseClient) {
        releaseWarehouseClient(storageProviderCredentials);
      }
    }
  }

  private StsClient acquireStsClientLocked(StorageProviderCredentials storageProviderCredentials) {
    if (closed) {
      throw new IllegalStateException("S3CredentialProvider is closed");
    }
    if (hasStaticKeys(storageProviderCredentials)) {
      String cacheKey = warehouseClientCacheKey(storageProviderCredentials);
      OwnedStsClient owned =
          ownedClients.computeIfAbsent(
              cacheKey,
              ignored -> new OwnedStsClient(buildWarehouseStsClient(storageProviderCredentials)));
      owned.inFlight++;
      retireSupersededWarehouseClients(storageProviderCredentials, cacheKey);
      return owned.client;
    }
    if (injectedStsClient != null) {
      return injectedStsClient;
    }
    throw new IllegalStateException(
        "Iceberg S3 credential vending requires warehouse client keys or a shared StsClient");
  }

  private void releaseWarehouseClient(StorageProviderCredentials storageProviderCredentials) {
    String cacheKey = warehouseClientCacheKey(storageProviderCredentials);
    synchronized (lifecycle) {
      OwnedStsClient owned = ownedClients.get(cacheKey);
      if (owned == null) {
        return;
      }
      owned.inFlight--;
      closeOwnedIfIdleAndRetired(cacheKey, owned);
    }
  }

  /**
   * Drop rotated-out warehouse clients from the live map. Close immediately when nothing is using
   * them; otherwise mark retired and let {@link #releaseWarehouseClient} close after in-flight
   * {@code assumeRole} calls finish.
   */
  private void retireSupersededWarehouseClients(
      StorageProviderCredentials storageProviderCredentials, String keepKey) {
    String prefix =
        storageProviderCredentials.region + "|" + storageProviderCredentials.clientId + "|";
    List<String> superseded = new ArrayList<>();
    for (String key : ownedClients.keySet()) {
      if (key.startsWith(prefix) && !key.equals(keepKey)) {
        superseded.add(key);
      }
    }
    for (String key : superseded) {
      OwnedStsClient old = ownedClients.get(key);
      if (old == null) {
        continue;
      }
      old.retired = true;
      closeOwnedIfIdleAndRetired(key, old);
    }
  }

  private void closeOwnedIfIdleAndRetired(String cacheKey, OwnedStsClient owned) {
    if (!owned.retired || owned.inFlight > 0) {
      return;
    }
    ownedClients.remove(cacheKey, owned);
    closeQuietly(owned.client);
  }

  private static boolean hasStaticKeys(StorageProviderCredentials storageProviderCredentials) {
    return storageProviderCredentials.clientId != null
        && !storageProviderCredentials.clientId.isEmpty()
        && storageProviderCredentials.clientSecret != null
        && !storageProviderCredentials.clientSecret.isEmpty();
  }

  private static String warehouseClientCacheKey(
      StorageProviderCredentials storageProviderCredentials) {
    return storageProviderCredentials.region
        + "|"
        + storageProviderCredentials.clientId
        + "|"
        + sha256Hex(storageProviderCredentials.clientSecret);
  }

  private static String sha256Hex(String secret) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(secret.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required to key warehouse STS clients", e);
    }
  }

  private static StsClient buildWarehouseStsClient(
      StorageProviderCredentials storageProviderCredentials) {
    AwsBasicCredentials credentials =
        AwsBasicCredentials.create(
            storageProviderCredentials.clientId, storageProviderCredentials.clientSecret);
    return StsClient.builder()
        .region(Region.of(storageProviderCredentials.region))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
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

  @Override
  public void close() {
    synchronized (lifecycle) {
      closed = true;
      List<String> keys = new ArrayList<>(ownedClients.keySet());
      for (String key : keys) {
        OwnedStsClient owned = ownedClients.get(key);
        if (owned == null) {
          continue;
        }
        owned.retired = true;
        closeOwnedIfIdleAndRetired(key, owned);
      }
    }
  }

  private static void closeQuietly(@Nullable StsClient client) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (Exception ignored) {
      // Best-effort shutdown of warehouse-scoped STS clients.
    }
  }

  private static final class OwnedStsClient {
    private final StsClient client;
    private int inFlight;
    private boolean retired;

    private OwnedStsClient(StsClient client) {
      this.client = client;
    }
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
