package com.linkedin.gms.factory.ingestion;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.objectstorage.ObjectStorageClientFactory;
import com.linkedin.metadata.config.CliVersionMatrixConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.HttpUrlIngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource;
import com.linkedin.metadata.version.GitVersion;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;

/**
 * Direct unit tests for {@link IngestionCliVersionMatrixServiceFactory}. The contract under test is
 * backend selection by URI scheme, plus the guarantee that every unusable configuration degrades to
 * {@link NoOpIngestionCliVersionMatrixSource} instead of failing GMS startup.
 *
 * <p>{@code gs://} wiring is deliberately not covered: constructing the GCS client resolves ambient
 * Application Default Credentials, which is environment-dependent and would make the test flaky.
 * The read path it feeds is covered by {@link ObjectStorageIngestionCliVersionMatrixSourceTest}.
 */
public class IngestionCliVersionMatrixServiceFactoryTest {

  private IngestionCliVersionMatrixServiceFactory factory;
  private ConfigurationProvider configProvider;
  private IngestionConfiguration ingestionConfig;
  private GitVersion gitVersion;

  @BeforeMethod
  public void setUp() {
    factory = new IngestionCliVersionMatrixServiceFactory();
    configProvider = mock(ConfigurationProvider.class);
    ingestionConfig = new IngestionConfiguration();
    CliVersionMatrixConfiguration matrixConfig = new CliVersionMatrixConfiguration();
    matrixConfig.setRefreshSeconds(600);
    ingestionConfig.setCliVersionMatrix(matrixConfig);
    gitVersion = mock(GitVersion.class);

    when(configProvider.getIngestion()).thenReturn(ingestionConfig);
    // GitVersion.toConfig() is read by the service-construction bean; an empty fixture is fine
    // for the source-selection tests which only exercise ingestionCliVersionMatrixSource().
    when(gitVersion.toConfig()).thenReturn(Map.of("version", "test-server-1.0"));

    setField(factory, "configProvider", configProvider);
    setField(factory, "gitVersion", gitVersion);
  }

  // ---------------------------------------------------------------------------
  // No URI configured
  // ---------------------------------------------------------------------------

  @Test
  public void testMatrixSource_whenUriIsUnsetOrEmpty_wiresNoOp() {
    // Default state from setUp: uri null.
    assertTrue(
        factory.ingestionCliVersionMatrixSource() instanceof NoOpIngestionCliVersionMatrixSource,
        "An unset URI must wire NoOpIngestionCliVersionMatrixSource");

    ingestionConfig.getCliVersionMatrix().setUri("");
    assertTrue(
        factory.ingestionCliVersionMatrixSource() instanceof NoOpIngestionCliVersionMatrixSource,
        "An empty URI is treated the same as unset — NoOp");
  }

  // ---------------------------------------------------------------------------
  // Scheme selects the backend
  // ---------------------------------------------------------------------------

  @Test
  public void testMatrixSource_whenUriIsHttpOrHttps_wiresHttpSource() {
    // Both schemes must route to the HTTP source: HttpClient supports plain http, and matching only
    // https would silently fall through to object-storage parsing and disable the matrix.
    for (String uri :
        new String[] {
          "https://example.invalid/matrix.json", "http://example.invalid/matrix.json"
        }) {
      ingestionConfig.getCliVersionMatrix().setUri(uri);

      IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();
      try {
        assertTrue(
            source instanceof HttpUrlIngestionCliVersionMatrixSource,
            uri + " must wire HttpUrlIngestionCliVersionMatrixSource");
      } finally {
        shutdown(source);
      }
    }
  }

  @Test
  public void testMatrixSource_whenUriIsS3AndClientAvailable_wiresObjectStorageSource() {
    ingestionConfig.getCliVersionMatrix().setUri("s3://cli-version-matrix/matrix.json");
    setField(factory, "objectStorageClientFactory", factoryWithS3Client());

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();
    try {
      assertTrue(
          source instanceof ObjectStorageIngestionCliVersionMatrixSource,
          "an s3:// URI with an available S3 client wires the object-storage source");
    } finally {
      shutdown(source);
    }
  }

  @Test
  public void testMatrixSource_whenUriIsFile_wiresObjectStorageSource() throws Exception {
    Path dir = Files.createTempDirectory("cli-version-matrix");
    ingestionConfig.getCliVersionMatrix().setUri("file://" + dir.resolve("matrix.json"));

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();
    try {
      assertTrue(
          source instanceof ObjectStorageIngestionCliVersionMatrixSource,
          "a file:// URI wires the object-storage source");
    } finally {
      shutdown(source);
    }
  }

  // ---------------------------------------------------------------------------
  // Unusable configuration degrades to NoOp rather than failing startup
  // ---------------------------------------------------------------------------

  @Test
  public void testMatrixSource_whenS3ClientUnavailable_wiresNoOp() {
    // s3:// but no S3 client (AWS not configured) → application default, not a startup failure.
    ingestionConfig.getCliVersionMatrix().setUri("s3://cli-version-matrix/matrix.json");
    setField(factory, "objectStorageClientFactory", mock(ObjectStorageClientFactory.class));

    assertTrue(
        factory.ingestionCliVersionMatrixSource() instanceof NoOpIngestionCliVersionMatrixSource,
        "an s3:// URI with no S3 client wires a no-op source");
  }

  @Test
  public void testMatrixSource_whenUriHasNoObjectKey_wiresNoOp() {
    // A bucket root is not a readable document — the object key is required.
    ingestionConfig.getCliVersionMatrix().setUri("s3://cli-version-matrix");
    setField(factory, "objectStorageClientFactory", factoryWithS3Client());

    assertTrue(
        factory.ingestionCliVersionMatrixSource() instanceof NoOpIngestionCliVersionMatrixSource,
        "a bucket-only URI must degrade to a no-op, not attempt a read with an empty key");
  }

  @Test
  public void testMatrixSource_whenSchemeUnsupported_wiresNoOp() {
    ingestionConfig.getCliVersionMatrix().setUri("ftp://example.invalid/matrix.json");

    assertTrue(
        factory.ingestionCliVersionMatrixSource() instanceof NoOpIngestionCliVersionMatrixSource,
        "an unsupported scheme must degrade to a no-op, not fail startup");
  }

  @Test
  public void testMatrixSource_whenRefreshSecondsNotPositive_wiresNoOp() {
    // A non-positive refresh interval would make scheduleAtFixedRate throw in the source
    // constructor and fail GMS startup; the factory must degrade to a no-op instead.
    ingestionConfig.getCliVersionMatrix().setUri("s3://cli-version-matrix/matrix.json");
    ingestionConfig.getCliVersionMatrix().setRefreshSeconds(0);
    setField(factory, "objectStorageClientFactory", factoryWithS3Client());

    assertTrue(
        factory.ingestionCliVersionMatrixSource() instanceof NoOpIngestionCliVersionMatrixSource,
        "non-positive refreshSeconds must degrade to a no-op, not fail startup");
  }

  // ---------------------------------------------------------------------------
  // Service construction
  // ---------------------------------------------------------------------------

  @Test
  public void testGetInstance_buildsServiceWithServerVersionFromGitVersion() {
    ingestionConfig.setDeploymentId("test-deployment");
    when(gitVersion.toConfig()).thenReturn(Map.of("version", "1.5.0"));

    IngestionCliVersionMatrixService service =
        factory.getInstance(new NoOpIngestionCliVersionMatrixSource());

    assertNotNull(service);
    assertEquals(
        service.getServerVersion(), "1.5.0", "Service uses the version reported by GitVersion");
  }

  /**
   * The matrix source only ever reads, but S3ObjectStorageClient's constructor eagerly derives a
   * presigner from the client's resolved credentials and region — so the mock has to answer {@code
   * serviceClientConfiguration()} even though no request is ever issued.
   */
  private static ObjectStorageClientFactory factoryWithS3Client() {
    S3Client s3Client = mock(S3Client.class);
    when(s3Client.serviceClientConfiguration())
        .thenReturn(
            S3ServiceClientConfiguration.builder()
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build());
    ObjectStorageClientFactory clientFactory = mock(ObjectStorageClientFactory.class);
    when(clientFactory.createS3Client()).thenReturn(s3Client);
    return clientFactory;
  }

  /** Stops the background refresh thread a successfully-wired source starts in its constructor. */
  private static void shutdown(IngestionCliVersionMatrixSource source) {
    if (source instanceof ObjectStorageIngestionCliVersionMatrixSource s) {
      s.shutdown();
    } else if (source instanceof HttpUrlIngestionCliVersionMatrixSource s) {
      s.shutdown();
    }
  }

  /** Reflection helper — the factory's autowired fields are private, like every Spring bean. */
  private static void setField(Object target, String name, Object value) {
    try {
      Field f = target.getClass().getDeclaredField(name);
      f.setAccessible(true);
      f.set(target, value);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set field " + name, e);
    }
  }
}
