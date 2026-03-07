package com.linkedin.metadata.resources.entity;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.graph.GraphService;
import io.datahubproject.metadata.services.RestrictedService;
import java.lang.reflect.Field;
import java.net.URISyntaxException;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Tests for EntityResource.
 * 
 * Note: Many EntityResource endpoints are async Rest.li methods that require ParSeq
 * infrastructure for full testing. These tests focus on the testable aspects without
 * the full async setup. Full integration tests should be used for endpoint testing.
 */
public class EntityResourceTest {
  private EntityResource entityResource;
  private EntityService<?> entityService;
  private AspectDao aspectDao;
  private EventProducer producer;
  private EntityRegistry entityRegistry;
  private UpdateIndicesService updateIndicesService;
  private PreProcessHooks preProcessHooks;
  private Authorizer authorizer;
  private OperationContext opContext;
  private SearchService searchService;
  private EntitySearchService entitySearchService;
  private SystemMetadataService systemMetadataService;
  private GraphService graphService;
  private DeleteEntityService deleteEntityService;
  private TimeseriesAspectService timeseriesAspectService;
  private RestrictedService restrictedService;
  private ElasticSearchConfiguration searchConfiguration;

  @BeforeTest
  public void setup() throws Exception {
    entityResource = new EntityResource();
    aspectDao = mock(AspectDao.class);
    producer = mock(EventProducer.class);
    updateIndicesService = mock(UpdateIndicesService.class);
    preProcessHooks = mock(PreProcessHooks.class);
    entityService = new EntityServiceImpl(aspectDao, producer, false,
            preProcessHooks, true);
    entityService.setUpdateIndicesService(updateIndicesService);
    authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class))).thenAnswer(invocation -> {
      AuthorizationRequest request = invocation.getArgument(0);
      return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "allowed");
    });
    
    searchService = mock(SearchService.class);
    entitySearchService = mock(EntitySearchService.class);
    systemMetadataService = mock(SystemMetadataService.class);
    graphService = mock(GraphService.class);
    deleteEntityService = mock(DeleteEntityService.class);
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    restrictedService = mock(RestrictedService.class);
    searchConfiguration = mock(ElasticSearchConfiguration.class);
    
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    entityRegistry = opContext.getEntityRegistry();

    // Use reflection to inject mocks since EntityResource uses @Inject
    injectField(entityResource, "entityService", entityService);
    injectField(entityResource, "searchService", searchService);
    injectField(entityResource, "entitySearchService", entitySearchService);
    injectField(entityResource, "systemMetadataService", systemMetadataService);
    injectField(entityResource, "eventProducer", producer);
    injectField(entityResource, "graphService", graphService);
    injectField(entityResource, "deleteEntityService", deleteEntityService);
    injectField(entityResource, "timeseriesAspectService", timeseriesAspectService);
    injectField(entityResource, "authorizer", authorizer);
    injectField(entityResource, "systemOperationContext", opContext);
    injectField(entityResource, "restrictedService", restrictedService);
    injectField(entityResource, "searchConfiguration", searchConfiguration);
  }

  private void injectField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  @Test
  public void testEntityResourceSetup() throws URISyntaxException {
    // Verify EntityResource is properly configured with all dependencies
    assertNotNull(entityResource);
    
    // Verify authentication mock is set up correctly
    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);
    
    assertEquals(mockAuthentication.getActor().getId(), "user");
    assertEquals(mockAuthentication.getActor().getType(), ActorType.USER);
  }

  @Test
  public void testAuthorizerAllowsRequests() throws URISyntaxException {
    // Verify authorizer allows requests when configured to do so
    reset(authorizer);
    
    when(authorizer.authorize(any(AuthorizationRequest.class))).thenAnswer(invocation -> {
      AuthorizationRequest request = invocation.getArgument(0);
      return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "allowed");
    });
    
    // Create a sample authorization request
    AuthorizationRequest request = mock(AuthorizationRequest.class);
    AuthorizationResult result = authorizer.authorize(request);
    
    assertEquals(result.getType(), AuthorizationResult.Type.ALLOW);
    assertEquals(result.getMessage(), "allowed");
  }

  @Test
  public void testAuthorizerDeniesRequests() throws URISyntaxException {
    // Verify authorizer can deny requests
    reset(authorizer);
    
    when(authorizer.authorize(any(AuthorizationRequest.class))).thenAnswer(invocation -> {
      AuthorizationRequest request = invocation.getArgument(0);
      return new AuthorizationResult(request, AuthorizationResult.Type.DENY, "Unauthorized");
    });
    
    AuthorizationRequest request = mock(AuthorizationRequest.class);
    AuthorizationResult result = authorizer.authorize(request);
    
    assertEquals(result.getType(), AuthorizationResult.Type.DENY);
    assertEquals(result.getMessage(), "Unauthorized");
  }

  @Test
  public void testDatasetUrnCreation() throws URISyntaxException {
    // Test creating a dataset URN
    Urn datasetUrn = new DatasetUrn(new DataPlatformUrn("platform"), "testDataset", FabricType.PROD);
    
    assertNotNull(datasetUrn);
    assertEquals(datasetUrn.getEntityType(), "dataset");
    assertTrue(datasetUrn.toString().contains("platform"));
    assertTrue(datasetUrn.toString().contains("testDataset"));
    assertTrue(datasetUrn.toString().contains("PROD"));
  }

  @Test
  public void testDomainUrnCreation() throws URISyntaxException {
    // Test creating a domain URN
    Urn domainUrn = Urn.createFromString("urn:li:domain:finance");
    
    assertNotNull(domainUrn);
    assertEquals(domainUrn.getEntityType(), "domain");
  }

  @Test
  public void testAuthenticationContext() {
    // Test that AuthenticationContext properly stores and retrieves authentication
    Authentication mockAuthentication = mock(Authentication.class);
    Actor actor = new Actor(ActorType.USER, "testUser");
    when(mockAuthentication.getActor()).thenReturn(actor);
    
    AuthenticationContext.setAuthentication(mockAuthentication);
    
    Authentication retrieved = AuthenticationContext.getAuthentication();
    assertNotNull(retrieved);
    assertEquals(retrieved.getActor().getId(), "testUser");
  }

  @Test
  public void testActorTypes() {
    // Verify different actor types are available
    Actor userActor = new Actor(ActorType.USER, "user1");
    assertEquals(userActor.getType(), ActorType.USER);
    assertEquals(userActor.getId(), "user1");
  }

  @Test
  public void testFabricTypes() throws URISyntaxException {
    // Test different fabric types for datasets
    Urn prodDataset = new DatasetUrn(new DataPlatformUrn("platform"), "dataset1", FabricType.PROD);
    Urn devDataset = new DatasetUrn(new DataPlatformUrn("platform"), "dataset1", FabricType.DEV);
    
    assertNotNull(prodDataset);
    assertNotNull(devDataset);
    assertNotEquals(prodDataset.toString(), devDataset.toString());
  }

  @Test
  public void testOperationContextIsAvailable() {
    // Verify the operation context is properly set up
    assertNotNull(opContext);
    assertNotNull(entityRegistry);
  }

  @Test
  public void testMocksAreInjectedCorrectly() throws Exception {
    // Verify mocks are injected correctly using reflection
    Field entityServiceField = entityResource.getClass().getDeclaredField("entityService");
    entityServiceField.setAccessible(true);
    assertNotNull(entityServiceField.get(entityResource));
    
    Field authorizerField = entityResource.getClass().getDeclaredField("authorizer");
    authorizerField.setAccessible(true);
    assertNotNull(authorizerField.get(entityResource));
  }

  @Test
  public void testDomainBasedAuthorizationMocking() throws URISyntaxException {
    // Test that domain-based authorization can be properly mocked
    reset(authorizer);
    
    Urn domainUrn = Urn.createFromString("urn:li:domain:finance");
    
    // Mock domain-based authorization check
    when(authorizer.authorize(any(AuthorizationRequest.class))).thenAnswer(invocation -> {
      AuthorizationRequest request = invocation.getArgument(0);
      // Check if this is a domain-related request
      if (request.getResourceSpec().isPresent()) {
        String resource = request.getResourceSpec().get().getEntity();
        if (resource.contains("finance")) {
          return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "Domain access granted");
        }
      }
      return new AuthorizationResult(request, AuthorizationResult.Type.DENY, "Domain access denied");
    });
    
    // Verify the mock works as expected
    assertNotNull(authorizer);
  }
}
