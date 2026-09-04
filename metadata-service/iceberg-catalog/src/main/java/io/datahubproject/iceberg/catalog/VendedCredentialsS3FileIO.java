package io.datahubproject.iceberg.catalog;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Builds Iceberg {@link S3FileIO} from warehouse-vended static keys only.
 *
 * <p>Iceberg's default {@code S3FileIO#initialize} uses {@code
 * DefaultCredentialsProvider.builder()} when access keys are missing. On EKS IRSA that allocates a
 * new {@code StsAssumeRoleWithWebIdentityCredentialsProvider} + STS client per FileIO.
 *
 * <p>Callers that create many FileIOs for the same vended keys should pass a {@link
 * VendedS3ClientCache} so Apache HTTP clients are reused until the cache is closed (typically
 * catalog close).
 *
 * <p>{@link S3FileIO} constructed with a client supplier does not close that client on {@code
 * close()}. The no-cache {@link #create(Map)} path wraps the FileIO so the owned client is closed.
 * The cache path uses {@link #nonClosingView(S3Client)} so catalog close owns the HTTP client.
 */
final class VendedCredentialsS3FileIO {

  static final String ACCESS_KEY_ID = "s3.access-key-id";
  static final String SECRET_ACCESS_KEY = "s3.secret-access-key";
  static final String SESSION_TOKEN = "s3.session-token";
  static final String CLIENT_REGION = "client.region";

  private VendedCredentialsS3FileIO() {}

  @Nonnull
  static FileIO create(@Nonnull Map<String, String> creds) {
    S3Client client = buildS3Client(creds);
    S3FileIO io = new S3FileIO(() -> client);
    io.initialize(creds);
    return closingFileIO(io, client);
  }

  @Nonnull
  static FileIO create(@Nonnull Map<String, String> creds, @Nonnull VendedS3ClientCache cache) {
    S3Client shared = cache.getOrCreate(creds);
    S3Client view = nonClosingView(shared);
    S3FileIO io = new S3FileIO(() -> view);
    io.initialize(creds);
    return io;
  }

  @Nonnull
  static S3Client buildS3Client(@Nonnull Map<String, String> creds) {
    String accessKeyId = creds.get(ACCESS_KEY_ID);
    String secretAccessKey = creds.get(SECRET_ACCESS_KEY);
    if (accessKeyId == null
        || accessKeyId.isBlank()
        || secretAccessKey == null
        || secretAccessKey.isBlank()) {
      throw new IllegalStateException(
          "Iceberg S3 FileIO requires vended static credentials (s3.access-key-id /"
              + " s3.secret-access-key); refusing the AWS default credential chain");
    }
    String sessionToken = creds.get(SESSION_TOKEN);
    AwsCredentials awsCredentials =
        sessionToken != null && !sessionToken.isBlank()
            ? AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken)
            : AwsBasicCredentials.create(accessKeyId, secretAccessKey);
    var builder =
        S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(awsCredentials));
    String region = creds.get(CLIENT_REGION);
    if (region != null && !region.isBlank()) {
      builder.region(Region.of(region));
    }
    return builder.build();
  }

  static String cacheKey(@Nonnull Map<String, String> creds) {
    return String.join(
        "\0",
        nullToEmpty(creds.get(ACCESS_KEY_ID)),
        nullToEmpty(creds.get(SECRET_ACCESS_KEY)),
        nullToEmpty(creds.get(SESSION_TOKEN)),
        nullToEmpty(creds.get(CLIENT_REGION)));
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  /**
   * Closes the owned {@link S3Client} when Iceberg closes the FileIO. Supplier-constructed {@link
   * S3FileIO} does not close that client itself.
   */
  @Nonnull
  static FileIO closingFileIO(@Nonnull S3FileIO io, @Nonnull S3Client client) {
    return (FileIO)
        Proxy.newProxyInstance(
            FileIO.class.getClassLoader(),
            new Class<?>[] {FileIO.class},
            (proxy, method, args) -> {
              if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                try {
                  io.close();
                } finally {
                  client.close();
                }
                return null;
              }
              return invokeUnchecked(io, method, args);
            });
  }

  /**
   * Iceberg {@link S3FileIO#close()} does not close a client passed in via {@code
   * SerializableSupplier}. Swallow {@code close} so a catalog-scoped cache can own the real client.
   */
  @Nonnull
  static S3Client nonClosingView(@Nonnull S3Client client) {
    return (S3Client)
        Proxy.newProxyInstance(
            S3Client.class.getClassLoader(),
            new Class<?>[] {S3Client.class},
            (proxy, method, args) -> {
              if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                return null;
              }
              if (method.getDeclaringClass() == Object.class) {
                switch (method.getName()) {
                  case "equals":
                    return proxy == args[0];
                  case "hashCode":
                    return System.identityHashCode(proxy);
                  case "toString":
                    return "NonClosingS3Client(" + client + ")";
                  default:
                    break;
                }
              }
              return invokeUnchecked(client, method, args);
            });
  }

  private static Object invokeUnchecked(Object target, Method method, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw e;
    }
  }

  /** Process-local cache of vended-credential S3 clients; close to release HTTP resources. */
  static final class VendedS3ClientCache implements Closeable {
    private final ConcurrentHashMap<String, S3Client> clients = new ConcurrentHashMap<>();

    @Nonnull
    S3Client getOrCreate(@Nonnull Map<String, String> creds) {
      return clients.computeIfAbsent(cacheKey(creds), key -> buildS3Client(creds));
    }

    int size() {
      return clients.size();
    }

    @Override
    public void close() {
      for (S3Client client : clients.values()) {
        client.close();
      }
      clients.clear();
    }
  }
}
