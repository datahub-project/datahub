package com.linkedin.gms.factory.ingestion;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CliVersionMatrixConfiguration;
import com.linkedin.metadata.config.HttpMatrixSourceConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.ingestion.HttpUrlIngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.NoOpIngestionCliVersionMatrixSource;
import com.linkedin.metadata.version.GitVersion;
import java.lang.reflect.Field;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Direct unit tests for {@link IngestionCliVersionMatrixServiceFactory}. Covers the source
 * selection contract: explicit {@code source: "none"} is a kill-switch; otherwise the HTTP source
 * is wired when {@code http.url} is set and a no-op source is wired when the URL is empty.
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
    ingestionConfig.setCliVersionMatrix(new CliVersionMatrixConfiguration());
    ingestionConfig.getCliVersionMatrix().setHttp(new HttpMatrixSourceConfiguration());
    gitVersion = mock(GitVersion.class);

    when(configProvider.getIngestion()).thenReturn(ingestionConfig);
    // GitVersion.toConfig() is read by the service-construction bean; an empty fixture is fine
    // for the source-selection tests which only exercise ingestionCliVersionMatrixSource().
    when(gitVersion.toConfig()).thenReturn(Map.of("version", "test-server-1.0"));

    setField(factory, "configProvider", configProvider);
    setField(factory, "gitVersion", gitVersion);
  }

  // ---------------------------------------------------------------------------
  // URL controls HTTP vs NoOp when source is not "none"
  // ---------------------------------------------------------------------------

  @Test
  public void testMatrixSource_whenUrlIsNull_wiresNoOp() {
    // Default state from setUp: url null. Factory wires a no-op source.
    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof NoOpIngestionCliVersionMatrixSource,
        "An unset URL must wire NoOpIngestionCliVersionMatrixSource");
  }

  @Test
  public void testMatrixSource_whenUrlIsEmpty_wiresNoOp() {
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("");

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof NoOpIngestionCliVersionMatrixSource,
        "An empty URL is treated the same as unset — NoOp");
  }

  @Test
  public void testMatrixSource_whenUrlIsSet_wiresHttpUrlSource() {
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("file:///tmp/nonexistent.json");
    ingestionConfig.getCliVersionMatrix().getHttp().setRefreshSeconds(3600);

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof HttpUrlIngestionCliVersionMatrixSource,
        "A non-empty URL wires HttpUrlIngestionCliVersionMatrixSource");
  }

  @Test
  public void testMatrixSource_whenSourceIsExplicitHttp_wiresHttpUrlSource() {
    ingestionConfig.getCliVersionMatrix().setSource("http");
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("file:///tmp/nonexistent.json");
    ingestionConfig.getCliVersionMatrix().getHttp().setRefreshSeconds(3600);

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof HttpUrlIngestionCliVersionMatrixSource,
        "Explicit source='http' with a URL wires HttpUrlIngestionCliVersionMatrixSource");
  }

  // ---------------------------------------------------------------------------
  // Explicit source="none" is a kill-switch
  // ---------------------------------------------------------------------------

  @Test
  public void testMatrixSource_whenSourceIsNone_overridesUrlPresence() {
    ingestionConfig.getCliVersionMatrix().setSource("none");
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("file:///tmp/nonexistent.json");

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof NoOpIngestionCliVersionMatrixSource,
        "source='none' is a kill-switch that wins over a configured URL");
  }

  @Test
  public void testMatrixSource_noneIsCaseInsensitive() {
    ingestionConfig.getCliVersionMatrix().setSource("NONE");
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("file:///tmp/nonexistent.json");

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof NoOpIngestionCliVersionMatrixSource,
        "Operators may set NONE or none — both must short-circuit to NoOp");
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
