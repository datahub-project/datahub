package com.linkedin.gms.factory.ingestion;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.HttpUrlMatrixSource;
import com.linkedin.metadata.ingestion.IngestionVersionMatrixService;
import com.linkedin.metadata.ingestion.MatrixSource;
import com.linkedin.metadata.ingestion.NoOpMatrixSource;
import com.linkedin.metadata.version.GitVersion;
import java.lang.reflect.Field;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Direct unit tests for {@link IngestionVersionMatrixServiceFactory}. Exercises the branch that
 * picks {@link NoOpMatrixSource} vs {@link HttpUrlMatrixSource} based on whether {@code
 * versionMatrixUrl} is configured — the rest of the codebase only ever exercises the no-op path
 * (test contexts don't set the env var).
 */
public class IngestionVersionMatrixServiceFactoryTest {

  private IngestionVersionMatrixServiceFactory factory;
  private ConfigurationProvider configProvider;
  private IngestionConfiguration ingestionConfig;
  private GitVersion gitVersion;

  @BeforeMethod
  public void setUp() {
    factory = new IngestionVersionMatrixServiceFactory();
    configProvider = mock(ConfigurationProvider.class);
    ingestionConfig = new IngestionConfiguration();
    gitVersion = mock(GitVersion.class);

    when(configProvider.getIngestion()).thenReturn(ingestionConfig);
    // GitVersion.toConfig() is called for the server-version key. Returning an empty config is
    // fine for the matrixSource() bean; only getInstance() reads the server version.
    when(gitVersion.toConfig()).thenReturn(Map.of("version", "test-server-1.0"));

    setField(factory, "configProvider", configProvider);
    setField(factory, "gitVersion", gitVersion);
  }

  @Test
  public void testMatrixSource_whenUrlIsNull_wiresNoOp() {
    ingestionConfig.setVersionMatrixUrl(null);

    MatrixSource source = factory.matrixSource();

    assertTrue(
        source instanceof NoOpMatrixSource,
        "Unset versionMatrixUrl should wire NoOpMatrixSource (OSS-safe default)");
  }

  @Test
  public void testMatrixSource_whenUrlIsEmpty_wiresNoOp() {
    ingestionConfig.setVersionMatrixUrl("");

    MatrixSource source = factory.matrixSource();

    assertTrue(
        source instanceof NoOpMatrixSource,
        "Empty-string versionMatrixUrl should be treated like unset → NoOpMatrixSource");
  }

  @Test
  public void testMatrixSource_whenUrlIsSet_wiresHttpUrlSource() {
    // file:// URI is fine — the factory only inspects the string, not whether it's reachable.
    ingestionConfig.setVersionMatrixUrl("file:///tmp/nonexistent-matrix.json");
    ingestionConfig.setVersionMatrixRefreshSeconds(3600);
    ingestionConfig.setVersionMatrixAuthToken(null);

    MatrixSource source = factory.matrixSource();

    assertTrue(
        source instanceof HttpUrlMatrixSource,
        "Configured versionMatrixUrl should wire HttpUrlMatrixSource");
  }

  @Test
  public void testGetInstance_buildsServiceWithServerVersionFromGitVersion() {
    ingestionConfig.setVersionMatrixUrl(null);
    ingestionConfig.setDeploymentId("test-deployment");
    when(gitVersion.toConfig()).thenReturn(Map.of("version", "1.5.0"));

    IngestionVersionMatrixService service = factory.getInstance(new NoOpMatrixSource());

    assertNotNull(service);
    assertEquals(
        service.getServerVersion(),
        "1.5.0",
        "Service should be constructed with the GitVersion's reported version");
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
