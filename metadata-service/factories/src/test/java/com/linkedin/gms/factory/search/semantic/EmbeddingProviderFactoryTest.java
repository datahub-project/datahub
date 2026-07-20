package com.linkedin.gms.factory.search.semantic;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.embedding.AwsBedrockEmbeddingProvider;
import com.linkedin.metadata.search.embedding.CohereEmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.NoOpEmbeddingProvider;
import com.linkedin.metadata.search.embedding.OpenAIEmbeddingProvider;
import com.linkedin.metadata.search.embedding.VertexAiEmbeddingProvider;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.function.Supplier;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Unit tests for the vertex_ai branch in {@link EmbeddingProviderFactory}.
 *
 * <p>These tests call {@code createVertexAiProvider} directly (package-private) with a synthetic
 * {@link EmbeddingProviderConfiguration} so they run without a Spring context, a live GCP project,
 * or any network access. The {@link EmbeddingProviderFactory} is subclassed to stub out {@link
 * EmbeddingProviderFactory#buildVertexAiTokenSupplier()} so that no ADC resolution or token refresh
 * is attempted.
 */
public class EmbeddingProviderFactoryTest {

  /** Factory subclass that replaces ADC token resolution with a no-op stub. */
  private static class TestableFactory extends EmbeddingProviderFactory {
    @Override
    protected Supplier<String> buildVertexAiTokenSupplier() {
      return () -> "stub-token";
    }
  }

  private static EmbeddingProviderConfiguration configWithVertexAi(
      String projectId, String location, String model, int dims) {
    EmbeddingProviderConfiguration config = new EmbeddingProviderConfiguration();
    config.setType("vertex_ai");

    EmbeddingProviderConfiguration.VertexAiConfig v =
        new EmbeddingProviderConfiguration.VertexAiConfig();
    v.setProjectId(projectId);
    v.setLocation(location);
    v.setModel(model);
    v.setOutputDimensionality(dims);
    config.setVertexai(v);

    return config;
  }

  /**
   * Builds a {@link TestableFactory} with a mocked {@link ConfigurationProvider} injected into the
   * {@code configurationProvider} field, wired to return the given {@link
   * EmbeddingProviderConfiguration} through the full config chain.
   */
  private static TestableFactory factoryWithConfig(EmbeddingProviderConfiguration embeddingConfig)
      throws Exception {
    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(true);
    semanticConfig.setEmbeddingProvider(embeddingConfig);

    EntityIndexConfiguration entityIndexConfig = new EntityIndexConfiguration();
    entityIndexConfig.setSemanticSearch(semanticConfig);

    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setEntityIndex(entityIndexConfig);

    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    when(configProvider.getElasticSearch()).thenReturn(esConfig);

    TestableFactory factory = new TestableFactory();
    Field field = EmbeddingProviderFactory.class.getDeclaredField("configurationProvider");
    field.setAccessible(true);
    field.set(factory, configProvider);

    return factory;
  }

  @Test
  public void instantiatesVertexAiProvider() {
    EmbeddingProviderConfiguration config =
        configWithVertexAi("my-gcp-project", "us-central1", "gemini-embedding-001", 768);

    EmbeddingProviderFactory factory = new TestableFactory();
    Object provider = factory.createVertexAiProvider(config);

    assertNotNull(provider, "createVertexAiProvider must return a non-null provider");
    assertTrue(
        provider instanceof VertexAiEmbeddingProvider,
        "Provider must be a VertexAiEmbeddingProvider, got: " + provider.getClass().getName());
  }

  @Test
  public void rejectsVertexAiWithoutProjectId() {
    EmbeddingProviderConfiguration config =
        configWithVertexAi(null, "us-central1", "gemini-embedding-001", 768);

    EmbeddingProviderFactory factory = new TestableFactory();
    assertThrows(IllegalStateException.class, () -> factory.createVertexAiProvider(config));
  }

  @Test
  public void rejectsVertexAiWithBlankProjectId() {
    EmbeddingProviderConfiguration config =
        configWithVertexAi("  ", "us-central1", "gemini-embedding-001", 768);

    EmbeddingProviderFactory factory = new TestableFactory();
    assertThrows(IllegalStateException.class, () -> factory.createVertexAiProvider(config));
  }

  @Test
  public void rejectsVertexAiWithNullLocation() {
    EmbeddingProviderConfiguration config =
        configWithVertexAi("my-gcp-project", null, "gemini-embedding-001", 768);

    EmbeddingProviderFactory factory = new TestableFactory();
    assertThrows(IllegalStateException.class, () -> factory.createVertexAiProvider(config));
  }

  @Test
  public void rejectsVertexAiWithBlankLocation() {
    EmbeddingProviderConfiguration config =
        configWithVertexAi("my-gcp-project", "", "gemini-embedding-001", 768);

    EmbeddingProviderFactory factory = new TestableFactory();
    assertThrows(IllegalStateException.class, () -> factory.createVertexAiProvider(config));
  }

  @Test
  public void appliesDefaultModelWhenNull() {
    EmbeddingProviderConfiguration config =
        configWithVertexAi("my-gcp-project", "us-central1", null, 0);

    EmbeddingProviderFactory factory = new TestableFactory();
    Object provider = factory.createVertexAiProvider(config);

    assertNotNull(provider);
    assertTrue(provider instanceof VertexAiEmbeddingProvider);
  }

  @Test
  public void vertexAiDefaultDimensionFallback() {
    EmbeddingProviderConfiguration config =
        configWithVertexAi("my-gcp-project", "us-central1", "gemini-embedding-001", 0);

    EmbeddingProviderFactory factory = new TestableFactory();
    EmbeddingProvider provider = factory.createVertexAiProvider(config);

    // Verifies that outputDimensionality=0 does not blow up and still produces a valid provider.
    assertNotNull(provider);
    assertTrue(provider instanceof VertexAiEmbeddingProvider);
  }

  /**
   * Until the ingestion-side client supports propagating non-default dimensions, the factory must
   * reject any non-default outputDimensionality to prevent server/client embedding-dimension Guard
   * was removed — any positive outputDimensionality is now accepted.
   */
  @Test
  public void acceptsVertexAiWithNonDefaultDimensions() {
    EmbeddingProviderConfiguration config =
        configWithVertexAi("my-gcp-project", "us-central1", "gemini-embedding-001", 1024);

    EmbeddingProviderFactory factory = new TestableFactory();
    EmbeddingProvider provider = factory.createVertexAiProvider(config);
    assertNotNull(provider);
    assertTrue(provider instanceof VertexAiEmbeddingProvider);
  }

  /** Routes through {@code getInstance()} switch-case with type="vertex_ai". */
  @Test
  public void vertexAiProviderInstantiatedViaGetInstance() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithVertexAi("my-gcp-project", "us-central1", "gemini-embedding-001", 768);

    TestableFactory factory = factoryWithConfig(config);
    EmbeddingProvider provider = factory.getInstance();

    assertNotNull(provider);
    assertTrue(
        provider instanceof VertexAiEmbeddingProvider,
        "getInstance() with type=vertex_ai must return VertexAiEmbeddingProvider, got: "
            + provider.getClass().getName());
  }

  /**
   * Exercises the real {@link EmbeddingProviderFactory#buildVertexAiTokenSupplier()} with a mocked
   * {@link GoogleCredentials} static, verifying the happy path returns the bearer token from the
   * resolved {@link AccessToken}.
   */
  @Test
  public void buildVertexAiTokenSupplierReturnsBearerToken() throws Exception {
    try (MockedStatic<GoogleCredentials> mocked = Mockito.mockStatic(GoogleCredentials.class)) {
      GoogleCredentials credentials = mock(GoogleCredentials.class);
      mocked.when(GoogleCredentials::getApplicationDefault).thenReturn(credentials);
      when(credentials.createScoped(anyString())).thenReturn(credentials);

      AccessToken token = mock(AccessToken.class);
      when(token.getTokenValue()).thenReturn("fake-bearer-token-123");
      when(credentials.getAccessToken()).thenReturn(token);

      EmbeddingProviderFactory factory = new EmbeddingProviderFactory();
      Supplier<String> supplier = factory.buildVertexAiTokenSupplier();
      assertNotNull(supplier);

      String result = supplier.get();
      assertEquals(result, "fake-bearer-token-123");

      // refreshIfExpired is invoked once at construction (fail-fast) and once per supplier.get().
      verify(credentials, atLeastOnce()).refreshIfExpired();
    }
  }

  /**
   * When credentials return a null {@link AccessToken}, the supplier lambda must surface a clear
   * {@link RuntimeException} rather than NPE'ing into the caller.
   */
  @Test
  public void buildVertexAiTokenSupplierThrowsWhenAccessTokenIsNull() throws Exception {
    try (MockedStatic<GoogleCredentials> mocked = Mockito.mockStatic(GoogleCredentials.class)) {
      GoogleCredentials credentials = mock(GoogleCredentials.class);
      mocked.when(GoogleCredentials::getApplicationDefault).thenReturn(credentials);
      when(credentials.createScoped(anyString())).thenReturn(credentials);
      when(credentials.getAccessToken()).thenReturn(null);

      EmbeddingProviderFactory factory = new EmbeddingProviderFactory();
      Supplier<String> supplier = factory.buildVertexAiTokenSupplier();

      RuntimeException ex = expectThrows(RuntimeException.class, supplier::get);
      assertTrue(
          ex.getMessage().contains("null access token"),
          "expected 'null access token' in message, got: " + ex.getMessage());
    }
  }

  /**
   * If ADC resolution itself fails with an {@link IOException}, the factory should wrap it in an
   * {@link IllegalStateException} at construction time (fail fast).
   */
  @Test
  public void buildVertexAiTokenSupplierWrapsIoExceptionAtConstruction() {
    try (MockedStatic<GoogleCredentials> mocked = Mockito.mockStatic(GoogleCredentials.class)) {
      mocked
          .when(GoogleCredentials::getApplicationDefault)
          .thenThrow(new IOException("simulated ADC failure"));

      EmbeddingProviderFactory factory = new EmbeddingProviderFactory();
      IllegalStateException ex =
          expectThrows(IllegalStateException.class, factory::buildVertexAiTokenSupplier);
      assertTrue(
          ex.getMessage().contains("GCP credentials"),
          "expected 'GCP credentials' in message, got: " + ex.getMessage());
      assertTrue(
          ex.getCause() instanceof IOException,
          "expected IOException cause, got: " + ex.getCause());
    }
  }

  /**
   * If construction succeeds but the in-lambda {@code refreshIfExpired()} later throws, the
   * supplier must wrap the IOException in a RuntimeException with a clear message.
   */
  @Test
  public void buildVertexAiTokenSupplierWrapsIoExceptionDuringRefresh() throws Exception {
    try (MockedStatic<GoogleCredentials> mocked = Mockito.mockStatic(GoogleCredentials.class)) {
      GoogleCredentials credentials = mock(GoogleCredentials.class);
      mocked.when(GoogleCredentials::getApplicationDefault).thenReturn(credentials);
      when(credentials.createScoped(anyString())).thenReturn(credentials);

      // First refresh (at construction) succeeds; second refresh (in lambda) throws.
      doNothing()
          .doThrow(new IOException("token refresh failed"))
          .when(credentials)
          .refreshIfExpired();

      EmbeddingProviderFactory factory = new EmbeddingProviderFactory();
      Supplier<String> supplier = factory.buildVertexAiTokenSupplier();

      RuntimeException ex = expectThrows(RuntimeException.class, supplier::get);
      assertTrue(
          ex.getMessage().contains("Failed to obtain GCP access token"),
          "expected 'Failed to obtain GCP access token' in message, got: " + ex.getMessage());
      assertTrue(
          ex.getCause() instanceof IOException,
          "expected IOException cause, got: " + ex.getCause());
    }
  }

  /**
   * The {@code getInstance()} switch-case must throw {@link IllegalStateException} for any provider
   * type outside the supported set, surfacing the offending value in the message.
   */
  @Test
  public void getInstanceThrowsOnUnsupportedProviderType() throws Exception {
    EmbeddingProviderConfiguration config = new EmbeddingProviderConfiguration();
    config.setType("banana");

    TestableFactory factory = factoryWithConfig(config);
    IllegalStateException ex = expectThrows(IllegalStateException.class, factory::getInstance);
    assertTrue(
        ex.getMessage().contains("Unsupported"),
        "expected 'Unsupported' in message, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("banana"),
        "expected offending type 'banana' in message, got: " + ex.getMessage());
  }

  // ------- AWS Bedrock provider tests -------

  private static EmbeddingProviderConfiguration configWithBedrock(String region, String model) {
    EmbeddingProviderConfiguration config = new EmbeddingProviderConfiguration();
    config.setType("aws-bedrock");
    EmbeddingProviderConfiguration.BedrockConfig b =
        new EmbeddingProviderConfiguration.BedrockConfig();
    b.setAwsRegion(region);
    b.setModel(model);
    config.setBedrock(b);
    return config;
  }

  @Test
  public void instantiatesAwsBedrockProvider() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithBedrock("us-west-2", "cohere.embed-english-v3");

    TestableFactory factory = factoryWithConfig(config);
    EmbeddingProvider provider = factory.getInstance();

    assertNotNull(provider);
    assertTrue(
        provider instanceof AwsBedrockEmbeddingProvider,
        "expected AwsBedrockEmbeddingProvider, got: " + provider.getClass().getName());
  }

  /** Routes through {@code getInstance()} switch-case with type="aws-bedrock". */
  @Test
  public void awsBedrockProviderInstantiatedViaGetInstance() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithBedrock("us-east-1", "amazon.titan-embed-text-v2:0");

    TestableFactory factory = factoryWithConfig(config);
    EmbeddingProvider provider = factory.getInstance();

    assertNotNull(provider);
    assertTrue(provider instanceof AwsBedrockEmbeddingProvider);
  }

  // ------- OpenAI provider tests -------

  private static EmbeddingProviderConfiguration configWithOpenAi(
      String apiKey, String endpoint, String model) {
    EmbeddingProviderConfiguration config = new EmbeddingProviderConfiguration();
    config.setType("openai");
    EmbeddingProviderConfiguration.OpenAIConfig o =
        new EmbeddingProviderConfiguration.OpenAIConfig();
    o.setApiKey(apiKey);
    o.setEndpoint(endpoint);
    o.setModel(model);
    config.setOpenai(o);
    return config;
  }

  @Test
  public void instantiatesOpenAIProvider() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithOpenAi(
            "sk-test-fake-key", "https://api.openai.com/v1/embeddings", "text-embedding-3-small");

    TestableFactory factory = factoryWithConfig(config);
    EmbeddingProvider provider = factory.getInstance();

    assertNotNull(provider);
    assertTrue(
        provider instanceof OpenAIEmbeddingProvider,
        "expected OpenAIEmbeddingProvider, got: " + provider.getClass().getName());
  }

  /** OpenAI provider must reject construction without an API key (fail-fast). */
  @Test
  public void rejectsOpenAIWithoutApiKey() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithOpenAi(null, "https://api.openai.com/v1/embeddings", "text-embedding-3-small");

    TestableFactory factory = factoryWithConfig(config);
    IllegalStateException ex = expectThrows(IllegalStateException.class, factory::getInstance);
    assertTrue(
        ex.getMessage().contains("OpenAI API key"),
        "expected 'OpenAI API key' in message, got: " + ex.getMessage());
  }

  @Test
  public void rejectsOpenAIWithBlankApiKey() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithOpenAi("   ", "https://api.openai.com/v1/embeddings", "text-embedding-3-small");

    TestableFactory factory = factoryWithConfig(config);
    assertThrows(IllegalStateException.class, factory::getInstance);
  }

  /** Routes through {@code getInstance()} switch-case with type="openai". */
  @Test
  public void openAiProviderInstantiatedViaGetInstance() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithOpenAi(
            "sk-test-fake-key", "https://api.openai.com/v1/embeddings", "text-embedding-3-large");

    TestableFactory factory = factoryWithConfig(config);
    EmbeddingProvider provider = factory.getInstance();

    assertNotNull(provider);
    assertTrue(provider instanceof OpenAIEmbeddingProvider);
  }

  // ------- Cohere provider tests -------

  private static EmbeddingProviderConfiguration configWithCohere(
      String apiKey, String endpoint, String model) {
    EmbeddingProviderConfiguration config = new EmbeddingProviderConfiguration();
    config.setType("cohere");
    EmbeddingProviderConfiguration.CohereConfig c =
        new EmbeddingProviderConfiguration.CohereConfig();
    c.setApiKey(apiKey);
    c.setEndpoint(endpoint);
    c.setModel(model);
    config.setCohere(c);
    return config;
  }

  @Test
  public void instantiatesCohereProvider() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithCohere("fake-cohere-key", "https://api.cohere.ai/v1/embed", "embed-english-v3.0");

    TestableFactory factory = factoryWithConfig(config);
    EmbeddingProvider provider = factory.getInstance();

    assertNotNull(provider);
    assertTrue(
        provider instanceof CohereEmbeddingProvider,
        "expected CohereEmbeddingProvider, got: " + provider.getClass().getName());
  }

  /** Cohere provider must reject construction without an API key (fail-fast). */
  @Test
  public void rejectsCohereWithoutApiKey() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithCohere(null, "https://api.cohere.ai/v1/embed", "embed-english-v3.0");

    TestableFactory factory = factoryWithConfig(config);
    IllegalStateException ex = expectThrows(IllegalStateException.class, factory::getInstance);
    assertTrue(
        ex.getMessage().contains("Cohere API key"),
        "expected 'Cohere API key' in message, got: " + ex.getMessage());
  }

  @Test
  public void rejectsCohereWithBlankApiKey() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithCohere("   ", "https://api.cohere.ai/v1/embed", "embed-english-v3.0");

    TestableFactory factory = factoryWithConfig(config);
    assertThrows(IllegalStateException.class, factory::getInstance);
  }

  /** Routes through {@code getInstance()} switch-case with type="cohere". */
  @Test
  public void cohereProviderInstantiatedViaGetInstance() throws Exception {
    EmbeddingProviderConfiguration config =
        configWithCohere(
            "fake-cohere-key", "https://api.cohere.ai/v1/embed", "embed-multilingual-v3.0");

    TestableFactory factory = factoryWithConfig(config);
    EmbeddingProvider provider = factory.getInstance();

    assertNotNull(provider);
    assertTrue(provider instanceof CohereEmbeddingProvider);
  }

  // ------- getInstance() NoOp paths -------

  /**
   * When the entire {@code semanticSearch} block is missing from configuration, {@code
   * getInstance()} returns a NoOp provider rather than failing.
   */
  @Test
  public void getInstanceReturnsNoOpWhenSemanticSearchConfigNull() throws Exception {
    EntityIndexConfiguration entityIndexConfig = new EntityIndexConfiguration();
    entityIndexConfig.setSemanticSearch(null);

    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setEntityIndex(entityIndexConfig);

    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    when(configProvider.getElasticSearch()).thenReturn(esConfig);

    TestableFactory factory = new TestableFactory();
    Field field = EmbeddingProviderFactory.class.getDeclaredField("configurationProvider");
    field.setAccessible(true);
    field.set(factory, configProvider);

    EmbeddingProvider provider = factory.getInstance();

    assertNotNull(provider);
    assertTrue(
        provider instanceof NoOpEmbeddingProvider,
        "expected NoOpEmbeddingProvider, got: " + provider.getClass().getName());
  }

  /**
   * When semantic search is explicitly disabled (enabled=false), {@code getInstance()} returns a
   * NoOp provider regardless of any other embedding config.
   */
  @Test
  public void getInstanceReturnsNoOpWhenSemanticSearchDisabled() throws Exception {
    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(false);
    // Set an embeddingProvider too so we prove it's the disabled flag that wins, not missing
    // config.
    semanticConfig.setEmbeddingProvider(
        configWithVertexAi("my-gcp-project", "us-central1", "gemini-embedding-001", 768));

    EntityIndexConfiguration entityIndexConfig = new EntityIndexConfiguration();
    entityIndexConfig.setSemanticSearch(semanticConfig);

    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setEntityIndex(entityIndexConfig);

    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    when(configProvider.getElasticSearch()).thenReturn(esConfig);

    TestableFactory factory = new TestableFactory();
    Field field = EmbeddingProviderFactory.class.getDeclaredField("configurationProvider");
    field.setAccessible(true);
    field.set(factory, configProvider);

    EmbeddingProvider provider = factory.getInstance();

    assertNotNull(provider);
    assertTrue(
        provider instanceof NoOpEmbeddingProvider,
        "expected NoOpEmbeddingProvider, got: " + provider.getClass().getName());
  }
}
