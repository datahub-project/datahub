package com.linkedin.datahub.graphql.plugins;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GmsGraphQLEngineArgs;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.TypeRuntimeWiring;
import java.util.Collection;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for SemanticSearchPlugin */
public class SemanticSearchPluginTest {

  @Mock private SemanticSearchService mockSemanticSearchService;

  @Mock private ViewService mockViewService;

  @Mock private FormService mockFormService;

  @Mock private EntityClient mockEntityClient;

  @Mock private GmsGraphQLEngine mockEngine;

  private SemanticSearchPlugin plugin;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    plugin = new SemanticSearchPlugin();
  }

  @Test
  public void testInitWithSemanticSearchService() {
    // Given: Args with SemanticSearchService configured
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(mockSemanticSearchService);
    args.setViewService(mockViewService);
    args.setFormService(mockFormService);
    args.setEntityClient(mockEntityClient);

    // When
    plugin.init(args);

    // Then: Plugin should initialize successfully
    // (verified by subsequent operations working correctly)
    List<String> schemaFiles = plugin.getSchemaFiles();
    assertEquals(schemaFiles.size(), 1);
    assertEquals(schemaFiles.get(0), "semantic-search.graphql");
  }

  @Test
  public void testInitWithNullSemanticSearchService() {
    // Given: Args without SemanticSearchService
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(null);
    args.setViewService(mockViewService);
    args.setFormService(mockFormService);
    args.setEntityClient(mockEntityClient);

    // When
    plugin.init(args);

    // Then: Plugin should initialize gracefully without throwing
    List<String> schemaFiles = plugin.getSchemaFiles();
    assertTrue(schemaFiles.isEmpty());
  }

  @Test
  public void testGetSchemaFilesWithService() {
    // Given: Plugin initialized with SemanticSearchService
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(mockSemanticSearchService);
    args.setViewService(mockViewService);
    args.setFormService(mockFormService);
    args.setEntityClient(mockEntityClient);
    plugin.init(args);

    // When
    List<String> schemaFiles = plugin.getSchemaFiles();

    // Then
    assertNotNull(schemaFiles);
    assertEquals(schemaFiles.size(), 1);
    assertEquals(schemaFiles.get(0), "semantic-search.graphql");
  }

  @Test
  public void testGetSchemaFilesWithoutService() {
    // Given: Plugin initialized without SemanticSearchService
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(null);
    args.setViewService(mockViewService);
    args.setFormService(mockFormService);
    args.setEntityClient(mockEntityClient);
    plugin.init(args);

    // When
    List<String> schemaFiles = plugin.getSchemaFiles();

    // Then
    assertNotNull(schemaFiles);
    assertTrue(schemaFiles.isEmpty());
  }

  @Test
  public void testGetLoadableTypesReturnsEmpty() {
    // Given: Plugin initialized
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(mockSemanticSearchService);
    args.setViewService(mockViewService);
    args.setFormService(mockFormService);
    args.setEntityClient(mockEntityClient);
    plugin.init(args);

    // When
    Collection<?> loadableTypes = plugin.getLoadableTypes();

    // Then: Semantic search doesn't introduce new loadable types
    assertNotNull(loadableTypes);
    assertTrue(loadableTypes.isEmpty());
  }

  @Test
  public void testGetEntityTypesReturnsEmpty() {
    // Given: Plugin initialized
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(mockSemanticSearchService);
    args.setViewService(mockViewService);
    args.setFormService(mockFormService);
    args.setEntityClient(mockEntityClient);
    plugin.init(args);

    // When
    Collection<?> entityTypes = plugin.getEntityTypes();

    // Then: Semantic search doesn't introduce new entity types
    assertNotNull(entityTypes);
    assertTrue(entityTypes.isEmpty());
  }

  @Test
  public void testConfigureExtraResolversWithService() {
    // Given: Plugin initialized with SemanticSearchService
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(mockSemanticSearchService);
    args.setViewService(mockViewService);
    args.setFormService(mockFormService);
    args.setEntityClient(mockEntityClient);
    plugin.init(args);

    RuntimeWiring.Builder mockBuilder = mock(RuntimeWiring.Builder.class);
    TypeRuntimeWiring.Builder mockTypeBuilder = mock(TypeRuntimeWiring.Builder.class);
    when(mockBuilder.type(eq("Query"), any()))
        .thenAnswer(
            invocation -> {
              java.util.function.UnaryOperator<TypeRuntimeWiring.Builder> configurator =
                  invocation.getArgument(1);
              configurator.apply(mockTypeBuilder);
              return mockBuilder;
            });
    when(mockTypeBuilder.dataFetcher(anyString(), any(DataFetcher.class)))
        .thenReturn(mockTypeBuilder);

    // When
    plugin.configureExtraResolvers(mockBuilder, mockEngine);

    // Then: Should register both semantic search resolvers
    verify(mockBuilder).type(eq("Query"), any());
    verify(mockTypeBuilder).dataFetcher(eq("semanticSearch"), any(DataFetcher.class));
    verify(mockTypeBuilder).dataFetcher(eq("semanticSearchAcrossEntities"), any(DataFetcher.class));
  }

  @Test
  public void testConfigureExtraResolversWithoutService() {
    // Given: Plugin initialized without SemanticSearchService
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(null);
    args.setViewService(mockViewService);
    args.setFormService(mockFormService);
    args.setEntityClient(mockEntityClient);
    plugin.init(args);

    RuntimeWiring.Builder mockBuilder = mock(RuntimeWiring.Builder.class);

    // When
    plugin.configureExtraResolvers(mockBuilder, mockEngine);

    // Then: Should NOT register any resolvers
    verify(mockBuilder, never()).type(anyString(), any());
  }

  @Test
  public void testPluginWithAllNullServices() {
    // Given: Args with all services null
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(null);
    args.setViewService(null);
    args.setFormService(null);
    args.setEntityClient(null);

    // When
    plugin.init(args);

    // Then: Should initialize without error
    assertTrue(plugin.getSchemaFiles().isEmpty());
    assertTrue(plugin.getLoadableTypes().isEmpty());
    assertTrue(plugin.getEntityTypes().isEmpty());
  }

  @Test
  public void testResolverRegistrationOrder() {
    // Given: Plugin initialized with SemanticSearchService
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setSemanticSearchService(mockSemanticSearchService);
    args.setViewService(mockViewService);
    args.setFormService(mockFormService);
    args.setEntityClient(mockEntityClient);
    plugin.init(args);

    RuntimeWiring.Builder mockBuilder = mock(RuntimeWiring.Builder.class);
    TypeRuntimeWiring.Builder mockTypeBuilder = mock(TypeRuntimeWiring.Builder.class);
    when(mockBuilder.type(eq("Query"), any()))
        .thenAnswer(
            invocation -> {
              java.util.function.UnaryOperator<TypeRuntimeWiring.Builder> configurator =
                  invocation.getArgument(1);
              configurator.apply(mockTypeBuilder);
              return mockBuilder;
            });
    when(mockTypeBuilder.dataFetcher(anyString(), any(DataFetcher.class)))
        .thenReturn(mockTypeBuilder);

    // Capture the resolver names in order
    ArgumentCaptor<String> resolverNameCaptor = ArgumentCaptor.forClass(String.class);

    // When
    plugin.configureExtraResolvers(mockBuilder, mockEngine);

    // Then: Verify correct resolver names are registered
    verify(mockTypeBuilder, org.mockito.Mockito.times(2))
        .dataFetcher(resolverNameCaptor.capture(), any(DataFetcher.class));

    List<String> registeredResolvers = resolverNameCaptor.getAllValues();
    assertTrue(registeredResolvers.contains("semanticSearch"));
    assertTrue(registeredResolvers.contains("semanticSearchAcrossEntities"));
  }
}
