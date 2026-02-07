package com.datahub.authorization;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for domain-based authorization in DomainAuthorizationHelper, specifically the
 * authorizeWithDomains method.
 */
public class DomainAuthorizationHelperTest {

  private OperationContext opContext;
  @Mock private Authorizer mockAuthorizer;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private EntitySpec mockEntitySpec;
  @Mock private AspectRetriever mockAspectRetriever;

  private static final String DATASET_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)";
  private static final String DOMAIN_URN_STRING = "urn:li:domain:finance";
  private static final String DOMAIN_URN_STRING_2 = "urn:li:domain:marketing";

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Configure mock authorizer to allow all authorization requests
    when(mockAuthorizer.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    // Create base test context
    OperationContext baseContext = TestOperationContexts.systemContextNoSearchAuthorization();

    // Wrap with our mock authorizer to control authorization behavior
    opContext =
        OperationContext.asSession(
            baseContext,
            RequestContext.TEST,
            mockAuthorizer, // Use our mock authorizer that returns ALLOW
            baseContext.getSessionAuthentication(),
            true);

    // Mock entity registry to return entity spec
    when(mockEntityRegistry.getEntitySpec(anyString())).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getKeyAspectSpec()).thenReturn(null);

    // Mock aspect retriever to return empty map by default (entity doesn't exist)
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());
  }

  @Test
  public void testAuthorizeWithDomains_NoDomainMap() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    // Call with null domains map - should use standard authorization
    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), null, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "Should be authorized with standard auth");
  }

  @Test
  public void testAuthorizeWithDomains_EmptyDomainMap() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    // Call with empty domains map - should use standard authorization
    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext,
            mockEntityRegistry,
            List.of(mcp),
            Collections.emptyMap(),
            mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "Should be authorized with standard auth");
  }

  @Test
  public void testAuthorizeWithDomains_WithDomains_Authorized() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(datasetUrn, Set.of(domainUrn));

    // Mock aspect retriever to return empty map (entity doesn't exist, so only new domain check)
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), domainsByEntity, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "Should be authorized with domain auth");
  }

  @Test
  public void testAuthorizeWithDomains_WithDomains_DeniedScenario() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(datasetUrn, Set.of(domainUrn));

    // Override mock authorizer to DENY for this test
    when(mockAuthorizer.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, "Denied"));

    // Mock aspect retriever to return empty map (entity doesn't exist)
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());

    // Execute - should handle authorization check gracefully
    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), domainsByEntity, mockAspectRetriever);

    // Verify method completes without exception
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertFalse(results.get(mcp), "Should be denied");

    // Reset mock authorizer back to ALLOW for other tests
    when(mockAuthorizer.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  @Test
  public void testAuthorizeWithDomains_MixedEntities() throws Exception {
    Urn dataset1Urn = Urn.createFromString(DATASET_URN_STRING);
    Urn dataset2Urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test2,PROD)");
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setEntityUrn(dataset1Urn);
    mcp1.setEntityType("dataset");
    mcp1.setChangeType(ChangeType.UPSERT);

    MetadataChangeProposal mcp2 = new MetadataChangeProposal();
    mcp2.setEntityUrn(dataset2Urn);
    mcp2.setEntityType("dataset");
    mcp2.setChangeType(ChangeType.UPSERT);

    // Only dataset1 has domains
    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(dataset1Urn, Set.of(domainUrn));

    // Mock aspect retriever
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext,
            mockEntityRegistry,
            List.of(mcp1, mcp2),
            domainsByEntity,
            mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 2);
    assertTrue(results.get(mcp1), "Dataset with domain should be authorized");
    assertTrue(results.get(mcp2), "Dataset without domain should be authorized");
  }

  @Test
  public void testAuthorizeWithDomains_DifferentOperations() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal createMcp = new MetadataChangeProposal();
    createMcp.setEntityUrn(datasetUrn);
    createMcp.setEntityType("dataset");
    createMcp.setChangeType(ChangeType.CREATE);

    MetadataChangeProposal updateMcp = new MetadataChangeProposal();
    updateMcp.setEntityUrn(datasetUrn);
    updateMcp.setEntityType("dataset");
    updateMcp.setChangeType(ChangeType.UPSERT);

    MetadataChangeProposal deleteMcp = new MetadataChangeProposal();
    deleteMcp.setEntityUrn(datasetUrn);
    deleteMcp.setEntityType("dataset");
    deleteMcp.setChangeType(ChangeType.DELETE);

    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(datasetUrn, Set.of(domainUrn));

    // Mock aspect retriever
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext,
            mockEntityRegistry,
            List.of(createMcp, updateMcp, deleteMcp),
            domainsByEntity,
            mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 3);
    assertTrue(results.get(createMcp), "CREATE should be authorized");
    assertTrue(results.get(updateMcp), "UPDATE should be authorized");
    assertTrue(results.get(deleteMcp), "DELETE should be authorized");
  }

  @Test
  public void testAuthorizeWithDomains_MultipleDomains() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domain1Urn = Urn.createFromString(DOMAIN_URN_STRING);
    Urn domain2Urn = Urn.createFromString(DOMAIN_URN_STRING_2);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(datasetUrn, Set.of(domain1Urn, domain2Urn));

    // Mock aspect retriever
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), domainsByEntity, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "Should be authorized with multiple domains");
  }

  @Test
  public void testAuthorizeWithDomains_EmptyMCPList() {
    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, Collections.emptyList(), null, mockAspectRetriever);

    assertNotNull(results);
    assertTrue(results.isEmpty());
  }

  @Test
  public void testAuthorizeWithDomains_RestateChangeType() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.RESTATE);

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), null, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "RESTATE should map to UPDATE operation and be authorized");
  }

  @Test
  public void testAuthorizeWithDomains_PatchChangeType() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.PATCH);

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), null, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "PATCH should map to UPDATE operation and be authorized");
  }

  @Test
  public void testAuthorizeWithDomains_CreateEntityChangeType() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.CREATE_ENTITY);

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), null, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "CREATE_ENTITY should map to CREATE operation and be authorized");
  }
}
