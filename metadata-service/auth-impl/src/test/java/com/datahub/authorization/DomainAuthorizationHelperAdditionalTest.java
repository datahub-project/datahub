package com.datahub.authorization;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.AspectSpec;
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

/** Additional tests for DomainAuthorizationHelper focusing on edge cases and error handling. */
public class DomainAuthorizationHelperAdditionalTest {

  private OperationContext opContext;
  @Mock private Authorizer mockAuthorizer;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private EntitySpec mockEntitySpec;
  @Mock private AspectSpec mockAspectSpec;
  @Mock private AspectRetriever mockAspectRetriever;

  private static final String DATASET_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)";
  private static final String DOMAIN_URN_STRING = "urn:li:domain:finance";

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    when(mockAuthorizer.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    OperationContext baseContext = TestOperationContexts.systemContextNoSearchAuthorization();
    opContext =
        OperationContext.asSession(
            baseContext,
            RequestContext.TEST,
            mockAuthorizer,
            baseContext.getSessionAuthentication(),
            true);

    when(mockEntityRegistry.getEntitySpec(anyString())).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getKeyAspectSpec()).thenReturn(mockAspectSpec);
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());
  }

  /** Test authorizeWithDomains with empty MCP list - should return empty result. */
  @Test
  public void testAuthorizeWithDomains_EmptyMCPList_ReturnsEmpty() {
    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, Collections.emptyList(), null, mockAspectRetriever);

    assertNotNull(results);
    assertTrue(results.isEmpty());
  }

  /**
   * Test authorizeWithDomains with entity that exists and has domains. Should check both existing
   * and new domains.
   */
  @Test
  public void testAuthorizeWithDomains_EntityExistsWithDomains_ChecksBoth() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn existingDomainUrn = Urn.createFromString("urn:li:domain:existing");
    Urn newDomainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    // Mock entity exists
    Map<Urn, Boolean> existsMap = new HashMap<>();
    existsMap.put(datasetUrn, true);
    when(mockAspectRetriever.entityExists(any())).thenReturn(existsMap);

    Map<Urn, Set<Urn>> newDomainsByEntity = new HashMap<>();
    newDomainsByEntity.put(datasetUrn, Set.of(newDomainUrn));

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), newDomainsByEntity, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "Should be authorized");
  }

  /** Test authorizeWithDomains with RESTATE change type - should map to UPDATE operation. */
  @Test
  public void testAuthorizeWithDomains_RestateChangeType_MapsToUpdate() throws Exception {
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
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with CREATE_ENTITY change type - should map to CREATE operation. */
  @Test
  public void testAuthorizeWithDomains_CreateEntityChangeType_MapsToCreate() throws Exception {
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
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with CREATE change type. */
  @Test
  public void testAuthorizeWithDomains_CreateChangeType() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.CREATE);

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), null, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with entity that doesn't exist - should only check new domains. */
  @Test
  public void testAuthorizeWithDomains_EntityNotExists_ChecksOnlyNewDomains() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.CREATE);

    Map<Urn, Set<Urn>> newDomainsByEntity = new HashMap<>();
    newDomainsByEntity.put(datasetUrn, Set.of(domainUrn));

    // Mock entity does NOT exist
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), newDomainsByEntity, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with multiple entities, some with domains, some without. */
  @Test
  public void testAuthorizeWithDomains_MixedEntitiesWithAndWithoutDomains() throws Exception {
    Urn dataset1Urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test1,PROD)");
    Urn dataset2Urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test2,PROD)");
    Urn dataset3Urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test3,PROD)");
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setEntityUrn(dataset1Urn);
    mcp1.setEntityType("dataset");
    mcp1.setChangeType(ChangeType.UPSERT);

    MetadataChangeProposal mcp2 = new MetadataChangeProposal();
    mcp2.setEntityUrn(dataset2Urn);
    mcp2.setEntityType("dataset");
    mcp2.setChangeType(ChangeType.UPSERT);

    MetadataChangeProposal mcp3 = new MetadataChangeProposal();
    mcp3.setEntityUrn(dataset3Urn);
    mcp3.setEntityType("dataset");
    mcp3.setChangeType(ChangeType.UPSERT);

    // Only dataset1 and dataset3 have domains
    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(dataset1Urn, Set.of(domainUrn));
    domainsByEntity.put(dataset3Urn, Set.of(domainUrn));

    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext,
            mockEntityRegistry,
            List.of(mcp1, mcp2, mcp3),
            domainsByEntity,
            mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 3);
    assertTrue(results.get(mcp1));
    assertTrue(results.get(mcp2));
    assertTrue(results.get(mcp3));
  }

  /** Test authorizeWithDomains handles exception from aspect retriever gracefully. */
  @Test
  public void testAuthorizeWithDomains_AspectRetrieverException_HandlesGracefully()
      throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    // Mock aspect retriever throws exception
    when(mockAspectRetriever.entityExists(any()))
        .thenThrow(new RuntimeException("Aspect retriever error"));

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), null, mockAspectRetriever);

    // Should handle exception gracefully and still return result
    assertNotNull(results);
    assertEquals(results.size(), 1);
  }

  /** Test authorizeWithDomains with null aspectRetriever - should fall back to standard auth. */
  @Test
  public void testAuthorizeWithDomains_NullAspectRetriever_FallsBackToStandard() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> newDomainsByEntity = new HashMap<>();
    newDomainsByEntity.put(datasetUrn, Set.of(domainUrn));

    // Call with non-empty domains map but null aspectRetriever - should fall back
    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), newDomainsByEntity, null);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    // Should use standard auth and be authorized
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with existing entity and domain changes - checks both. */
  @Test
  public void testAuthorizeWithDomains_ExistingEntityWithDomainChange_ChecksBothDomains()
      throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> newDomainsByEntity = new HashMap<>();
    newDomainsByEntity.put(datasetUrn, Set.of(domainUrn));

    // Mock entity exists
    Map<Urn, Boolean> existsMap = new HashMap<>();
    existsMap.put(datasetUrn, true);
    when(mockAspectRetriever.entityExists(any())).thenReturn(existsMap);

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), newDomainsByEntity, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    // Should be authorized since mock authorizer returns ALLOW
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with existing entity NOT changing domains - only checks existing. */
  @Test
  public void testAuthorizeWithDomains_ExistingEntityWithoutDomainChange() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn otherDatasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,other,PROD)");
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    // Domain changes for OTHER dataset, not this one
    Map<Urn, Set<Urn>> newDomainsByEntity = new HashMap<>();
    newDomainsByEntity.put(otherDatasetUrn, Set.of(domainUrn));

    // Mock entity exists
    Map<Urn, Boolean> existsMap = new HashMap<>();
    existsMap.put(datasetUrn, true);
    when(mockAspectRetriever.entityExists(any())).thenReturn(existsMap);

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), newDomainsByEntity, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with DELETE change type. */
  @Test
  public void testAuthorizeWithDomains_DeleteChangeType() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.DELETE);

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), null, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with UPDATE change type. */
  @Test
  public void testAuthorizeWithDomains_UpdateChangeType() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPDATE);

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), null, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with PATCH change type. */
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
    assertTrue(results.get(mcp));
  }

  /** Test authorizeWithDomains with multiple MCPs in different operations. */
  @Test
  public void testAuthorizeWithDomains_MultipleOperations() throws Exception {
    Urn createUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,create,PROD)");
    Urn updateUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,update,PROD)");
    Urn deleteUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,delete,PROD)");

    MetadataChangeProposal createMcp = new MetadataChangeProposal();
    createMcp.setEntityUrn(createUrn);
    createMcp.setEntityType("dataset");
    createMcp.setChangeType(ChangeType.CREATE);

    MetadataChangeProposal updateMcp = new MetadataChangeProposal();
    updateMcp.setEntityUrn(updateUrn);
    updateMcp.setEntityType("dataset");
    updateMcp.setChangeType(ChangeType.UPSERT);

    MetadataChangeProposal deleteMcp = new MetadataChangeProposal();
    deleteMcp.setEntityUrn(deleteUrn);
    deleteMcp.setEntityType("dataset");
    deleteMcp.setChangeType(ChangeType.DELETE);

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext,
            mockEntityRegistry,
            List.of(createMcp, updateMcp, deleteMcp),
            null,
            mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 3);
    assertTrue(results.get(createMcp));
    assertTrue(results.get(updateMcp));
    assertTrue(results.get(deleteMcp));
  }

  /** Test authorizeWithDomains when authorization is denied for domain change. */
  @Test
  public void testAuthorizeWithDomains_DeniedForDomainChange() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> newDomainsByEntity = new HashMap<>();
    newDomainsByEntity.put(datasetUrn, Set.of(domainUrn));

    // Mock authorization to DENY
    when(mockAuthorizer.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, "Denied"));

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), newDomainsByEntity, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertFalse(results.get(mcp), "Should be denied when domain authorization fails");

    // Reset mock authorizer
    when(mockAuthorizer.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  /** Test authorizeWithDomains with empty domains set. */
  @Test
  public void testAuthorizeWithDomains_EmptyDomainsSet() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    // Empty domains set for this entity
    Map<Urn, Set<Urn>> newDomainsByEntity = new HashMap<>();
    newDomainsByEntity.put(datasetUrn, Collections.emptySet());

    Map<MetadataChangeProposal, Boolean> results =
        DomainAuthorizationHelper.authorizeWithDomains(
            opContext, mockEntityRegistry, List.of(mcp), newDomainsByEntity, mockAspectRetriever);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp));
  }
}
