package com.linkedin.metadata.search;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ElasticSearchIndexedTweakReplicasTest {

  // Test implementation of ElasticSearchIndexed
  private static class TestElasticSearchIndexed implements ElasticSearchIndexed {
    private final ESIndexBuilder indexBuilder;
    private final List<ReindexConfig> configs;
    private boolean throwOnBuildReindexConfigs = false;

    public TestElasticSearchIndexed(ESIndexBuilder indexBuilder, List<ReindexConfig> configs) {
      this.indexBuilder = indexBuilder;
      this.configs = configs;
    }

    public void setThrowOnBuildReindexConfigs(boolean throwOnBuildReindexConfigs) {
      this.throwOnBuildReindexConfigs = throwOnBuildReindexConfigs;
    }

    @Override
    public List<ReindexConfig> buildReindexConfigs(
        Collection<Pair<Urn, StructuredPropertyDefinition>> properties) throws IOException {
      if (throwOnBuildReindexConfigs) {
        throw new IOException("Test exception in buildReindexConfigs");
      }
      return configs;
    }

    @Override
    public ESIndexBuilder getIndexBuilder() {
      return indexBuilder;
    }

    @Override
    public void reindexAll(Collection<Pair<Urn, StructuredPropertyDefinition>> properties)
        throws IOException {
      // Not needed for this test
    }
  }

  @Mock private ESIndexBuilder mockIndexBuilder;

  @Mock private Urn mockUrn;

  @Mock private StructuredPropertyDefinition mockPropertyDef;

  @Mock private ReindexConfig mockReindexConfig1;

  @Mock private ReindexConfig mockReindexConfig2;

  private Set<Pair<Urn, StructuredPropertyDefinition>> properties;
  private TestElasticSearchIndexed testService;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    properties = new HashSet<>();
    properties.add(new Pair<>(mockUrn, mockPropertyDef));

    List<ReindexConfig> configs = new ArrayList<>();
    configs.add(mockReindexConfig1);
    configs.add(mockReindexConfig2);

    testService = new TestElasticSearchIndexed(mockIndexBuilder, configs);
  }

  @Test
  public void testTweakReplicasAll_Success() throws IOException {
    // Execute the method
    testService.tweakReplicasAll(properties, false);

    // Verify that tweakReplicas was called for each config
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig1, false);
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig2, false);
  }

  @Test
  public void testTweakReplicasAll_DryRunTrue() throws IOException {
    // Execute the method with dryRun=true
    testService.tweakReplicasAll(properties, true);

    // Verify that tweakReplicas was called with dryRun=true
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig1, true);
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig2, true);
  }

  @Test
  public void testTweakReplicasAll_EmptyConfigs() throws IOException {
    // Create a service with empty configs
    testService = new TestElasticSearchIndexed(mockIndexBuilder, Collections.emptyList());

    // Execute the method
    testService.tweakReplicasAll(properties, false);

    // Verify that tweakReplicas was not called
    verify(mockIndexBuilder, never()).tweakReplicas(any(), anyBoolean());
  }

  @SneakyThrows
  @Test
  public void testTweakReplicasAll_IOExceptionInBuildReindexConfigs() {
    // Set up the test service to throw an IOException
    testService.setThrowOnBuildReindexConfigs(true);
    try {
      testService.tweakReplicasAll(properties, false);
      fail("Expected RuntimeException was not thrown");
    } catch (RuntimeException exception) {
      // Verify the cause of the exception
      assertEquals(IOException.class, exception.getCause().getClass());
      assertEquals("Test exception in buildReindexConfigs", exception.getCause().getMessage());
      // Verify that tweakReplicas was not called
      verify(mockIndexBuilder, never()).tweakReplicas(any(), anyBoolean());
    }
  }

  @Test
  public void testTweakReplicasAll_ExceptionInTweakReplicas() throws IOException {
    // Set up the index builder to throw an exception
    doThrow(new RuntimeException("Tweak replicas failed"))
        .when(mockIndexBuilder)
        .tweakReplicas(mockReindexConfig1, false);
    try {
      testService.tweakReplicasAll(properties, false);
      fail("Expected RuntimeException was not thrown");
    } catch (RuntimeException exception) {
      // Verify the exception message
      assertEquals("Tweak replicas failed", exception.getMessage());
      // Verify that tweakReplicas was called for the first config but not the second
      verify(mockIndexBuilder).tweakReplicas(mockReindexConfig1, false);
      verify(mockIndexBuilder, never()).tweakReplicas(mockReindexConfig2, false);
    }
  }

  @Test
  public void testTweakReplicasAll_NullProperties() throws IOException {
    // Execute the method with null properties
    testService.tweakReplicasAll(null, false);

    // Verify that buildReindexConfigs was called with null
    // and tweakReplicas was called for each config
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig1, false);
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig2, false);
  }

  @Test
  public void testTweakReplicasAll_MultipleConfigs() throws IOException {
    // Add more configs
    List<ReindexConfig> manyConfigs = new ArrayList<>();
    manyConfigs.add(mockReindexConfig1);
    manyConfigs.add(mockReindexConfig2);

    ReindexConfig mockReindexConfig3 = mock(ReindexConfig.class);
    ReindexConfig mockReindexConfig4 = mock(ReindexConfig.class);
    manyConfigs.add(mockReindexConfig3);
    manyConfigs.add(mockReindexConfig4);

    testService = new TestElasticSearchIndexed(mockIndexBuilder, manyConfigs);

    // Execute the method
    testService.tweakReplicasAll(properties, false);

    // Verify that tweakReplicas was called for each config
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig1, false);
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig2, false);
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig3, false);
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig4, false);
  }

  @Test
  public void testTweakReplicasAll_ExceptionInMiddleOfProcessing() throws IOException {
    // Set up the index builder to throw an exception for the second config
    doNothing().when(mockIndexBuilder).tweakReplicas(mockReindexConfig1, false);
    doThrow(new RuntimeException("Error on second config"))
        .when(mockIndexBuilder)
        .tweakReplicas(mockReindexConfig2, false);
    try {
      testService.tweakReplicasAll(properties, false);
      fail("Expected RuntimeException was not thrown");
    } catch (RuntimeException exception) {
      // Verify the exception message
      assertEquals("Error on second config", exception.getMessage());
      // Verify that tweakReplicas was called for both configs
      verify(mockIndexBuilder).tweakReplicas(mockReindexConfig1, false);
      verify(mockIndexBuilder).tweakReplicas(mockReindexConfig2, false);
    }
  }

  @Test
  public void testTweakReplicasAll_MultiplePropertiesWithDryRun() throws IOException {
    // Create multiple properties
    Set<Pair<Urn, StructuredPropertyDefinition>> multipleProperties = new HashSet<>();
    multipleProperties.add(new Pair<>(mockUrn, mockPropertyDef));

    Urn mockUrn2 = mock(Urn.class);
    StructuredPropertyDefinition mockPropertyDef2 = mock(StructuredPropertyDefinition.class);
    multipleProperties.add(new Pair<>(mockUrn2, mockPropertyDef2));

    // Execute the method with dryRun=true
    testService.tweakReplicasAll(multipleProperties, true);

    // Verify that buildReindexConfigs was called with the multiple properties
    // and tweakReplicas was called for each config with dryRun=true
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig1, true);
    verify(mockIndexBuilder).tweakReplicas(mockReindexConfig2, true);
  }
}
