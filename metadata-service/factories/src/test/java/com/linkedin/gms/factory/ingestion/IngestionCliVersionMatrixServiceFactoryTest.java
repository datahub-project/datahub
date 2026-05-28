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
 * Direct unit tests for {@link IngestionCliVersionMatrixServiceFactory}. Covers the {@code
 * ingestion.cliVersionMatrix.source} discriminator (explicit {@code "http"} / {@code "none"}) and
 * the backward-compat auto-inference path (unset discriminator infers from {@code http.url}
 * presence so deployments that pre-date this discriminator keep working).
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
    // GitVersion.toConfig() is called for the server-version key. Returning an empty config is
    // fine for the ingestionCliVersionMatrixSource() bean; only getInstance() reads the server
    // version.
    when(gitVersion.toConfig()).thenReturn(Map.of("version", "test-server-1.0"));

    setField(factory, "configProvider", configProvider);
    setField(factory, "gitVersion", gitVersion);
  }

  // ---------------------------------------------------------------------------
  // Backward-compat auto-inference (source unset)
  // ---------------------------------------------------------------------------

  @Test
  public void testMatrixSource_whenSourceUnsetAndUrlNull_wiresNoOp() {
    // Default state from setUp: source unset, http.url null. Should infer "none".
    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof NoOpIngestionCliVersionMatrixSource,
        "Unset source + unset url should infer 'none' (OSS-safe default)");
  }

  @Test
  public void testMatrixSource_whenSourceUnsetAndUrlEmpty_wiresNoOp() {
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("");

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof NoOpIngestionCliVersionMatrixSource,
        "Unset source + empty url should infer 'none'");
  }

  @Test
  public void testMatrixSource_whenSourceUnsetButUrlPresent_infersHttp() {
    // Backward-compat: pre-discriminator deployments only set the URL. Should still wire HTTP.
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("file:///tmp/nonexistent.json");
    ingestionConfig.getCliVersionMatrix().getHttp().setRefreshSeconds(3600);

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof HttpUrlIngestionCliVersionMatrixSource,
        "URL-only configuration must keep wiring HTTP for pre-discriminator deployments");
  }

  // ---------------------------------------------------------------------------
  // Explicit discriminator
  // ---------------------------------------------------------------------------

  @Test
  public void testMatrixSource_explicitHttp_wiresHttpUrlSource() {
    ingestionConfig.getCliVersionMatrix().setSource("http");
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("file:///tmp/nonexistent.json");
    ingestionConfig.getCliVersionMatrix().getHttp().setRefreshSeconds(3600);

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof HttpUrlIngestionCliVersionMatrixSource,
        "Explicit source='http' with url present should wire HttpUrlIngestionCliVersionMatrixSource");
  }

  @Test
  public void testMatrixSource_explicitNone_overridesUrlPresence() {
    // Explicit "none" must win even when a URL is configured.
    ingestionConfig.getCliVersionMatrix().setSource("none");
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("file:///tmp/nonexistent.json");

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof NoOpIngestionCliVersionMatrixSource,
        "Explicit source='none' must override URL presence (kill-switch semantics)");
  }

  @Test
  public void testMatrixSource_sourceIsCaseInsensitive() {
    ingestionConfig.getCliVersionMatrix().setSource("HTTP");
    ingestionConfig.getCliVersionMatrix().getHttp().setUrl("file:///tmp/nonexistent.json");
    ingestionConfig.getCliVersionMatrix().getHttp().setRefreshSeconds(3600);

    IngestionCliVersionMatrixSource source = factory.ingestionCliVersionMatrixSource();

    assertTrue(
        source instanceof HttpUrlIngestionCliVersionMatrixSource,
        "Source discriminator should be case-insensitive (operators may set HTTP or http)");
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
