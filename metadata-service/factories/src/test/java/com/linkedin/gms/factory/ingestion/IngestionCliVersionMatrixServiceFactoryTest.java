package com.linkedin.gms.factory.ingestion;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.objectstorage.ObjectStorageClientFactory;
import com.linkedin.metadata.config.CliVersionMatrixConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.HttpMatrixDocumentReader;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.PollingIngestionCliVersionMatrixSource;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageLocation;
import com.linkedin.metadata.version.GitVersion;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Direct unit tests for {@link IngestionCliVersionMatrixServiceFactory}. The contract under test is
 * backend selection by URI scheme, plus the guarantee that every unusable configuration degrades to
 * {@link NoOpIngestionCliVersionMatrixSource} instead of failing GMS startup.
 *
 * <p>{@code gs://} wiring is deliberately not covered: constructing the GCS client resolves ambient
 * Application Default Credentials, which is environment-dependent and would make the test flaky.
 * The read path it feeds is covered by {@link ObjectStorageMatrixDocumentReaderTest}.
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
    setField(factory, "objectStorageClientFactory", factoryWithClient());
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
    // https would silently fall through to object-storage parsing and disable the matrix. Scheme
    // case must not matter either — URI schemes are case-insensitive per RFC 3986 §3.1, and an
    // uppercase one used to fall through to object-storage parsing and wire a no-op.
    for (String uri :
        new String[] {
          "https://example.invalid/matrix.json",
          "http://example.invalid/matrix.json",
          "HTTPS://example.invalid/matrix.json",
          "HtTp://example.invalid/matrix.json"
        }) {
      ingestionConfig.getCliVersionMatrix().setUri(uri);

      IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();
      try {
        assertTrue(
            source instanceof PollingIngestionCliVersionMatrixSource s
                && uri.equals(s.displayUri()),
            uri
                + " must wire a polling source over an "
                + HttpMatrixDocumentReader.class.getSimpleName());
      } finally {
        shutdown(source);
      }
    }
  }

  @Test
  public void testMatrixSource_whenUriIsS3AndClientAvailable_wiresObjectStorageSource() {
    ingestionConfig.getCliVersionMatrix().setUri("s3://cli-version-matrix/matrix.json");

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();
    try {
      assertTrue(
          source instanceof PollingIngestionCliVersionMatrixSource s
              && "s3://cli-version-matrix/matrix.json".equals(s.displayUri()),
          "an s3:// URI with an available S3 client wires a polling source over that URI");
    } finally {
      shutdown(source);
    }
  }

  @Test
  public void testMatrixSource_whenAuthTokenSetOnCloudUri_warnsButStillWires() {
    // authToken only reaches the HTTP reader, so it is ignored here — but ignoring it must stay a
    // warning, not a silent behaviour change: the source is still wired over the same URI.
    ingestionConfig.getCliVersionMatrix().setUri("s3://cli-version-matrix/matrix.json");
    ingestionConfig.getCliVersionMatrix().setAuthToken("token ghp_ignored");

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();
    try {
      assertTrue(
          source instanceof PollingIngestionCliVersionMatrixSource s
              && "s3://cli-version-matrix/matrix.json".equals(s.displayUri()),
          "an ignored authToken must not change which source is wired");
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
          source instanceof PollingIngestionCliVersionMatrixSource,
          "a file:// URI wires a polling source");
    } finally {
      shutdown(source);
    }
  }

  // ---------------------------------------------------------------------------
  // Unusable configuration degrades to NoOp rather than failing startup
  // ---------------------------------------------------------------------------

  @Test
  public void testMatrixSource_whenS3ClientUnavailable_wiresNoOp() {
    // s3:// but clientFor() yields null (AWS not configured) → application default, not a startup
    // failure. A bare mock returns null from every method, which is exactly that case.
    ingestionConfig.getCliVersionMatrix().setUri("s3://cli-version-matrix/matrix.json");
    setField(factory, "objectStorageClientFactory", mock(ObjectStorageClientFactory.class));

    assertTrue(
        factory.ingestionCliVersionMatrixSource() instanceof NoOpIngestionCliVersionMatrixSource,
        "an s3:// URI with no S3 client wires a no-op source");
  }

  @Test
  public void testMatrixSource_whenNoObjectStorageClientFactoryInContext_wiresNoOp() {
    // This factory is also loaded by contexts that never import ObjectStorageClientFactory — the
    // mae-consumer app context pulls it in via IngestionSchedulerFactory. A required injection
    // there
    // fails the whole context at startup, so the dependency is optional and its absence must
    // degrade
    // to the application default like any other unusable configuration.
    ingestionConfig.getCliVersionMatrix().setUri("s3://cli-version-matrix/matrix.json");
    setField(factory, "objectStorageClientFactory", null);

    assertTrue(
        factory.ingestionCliVersionMatrixSource() instanceof NoOpIngestionCliVersionMatrixSource,
        "a context without ObjectStorageClientFactory must wire a no-op, not fail startup");
  }

  @Test
  public void testMatrixSource_whenUriHasNoObjectKey_wiresNoOp() {
    // A bucket root is not a readable document — the object key is required.
    ingestionConfig.getCliVersionMatrix().setUri("s3://cli-version-matrix");

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

  /** A client factory that serves every location — the read itself is never exercised here. */
  private static ObjectStorageClientFactory factoryWithClient() {
    ObjectStorageClientFactory clientFactory = mock(ObjectStorageClientFactory.class);
    when(clientFactory.clientFor(any(ObjectStorageLocation.class)))
        .thenReturn(mock(ObjectStorageClient.class));
    return clientFactory;
  }

  /** Stops the background refresh thread a successfully-wired source starts in its constructor. */
  private static void shutdown(IngestionCliVersionMatrixSource source) {
    if (source instanceof PollingIngestionCliVersionMatrixSource s) {
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
