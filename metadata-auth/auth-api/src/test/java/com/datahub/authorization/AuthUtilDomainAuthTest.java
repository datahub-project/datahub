package com.datahub.authorization;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for domain-based authorization in AuthUtil, specifically the new
 * isAPIAuthorizedMCPsWithDomains method.
 */
public class AuthUtilDomainAuthTest {

  @Mock private AuthorizationSession mockSession;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private EntitySpec mockEntitySpec;

  private static final String DATASET_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)";
  private static final String DOMAIN_URN_STRING = "urn:li:domain:finance";
  private static final String DOMAIN_URN_STRING_2 = "urn:li:domain:marketing";

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Mock entity registry to return entity spec
    when(mockEntityRegistry.getEntitySpec(anyString())).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getKeyAspectSpec()).thenReturn(null);

    // Default authorization to ALLOW
    when(mockSession.authorize(anyString(), any(), any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_NoDomainMap() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    // Call with null domains map - should use standard authorization
    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp),
            null);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "Should be authorized with standard auth");
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_EmptyDomainMap() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    // Call with empty domains map - should use standard authorization
    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp),
            Collections.emptyMap());

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "Should be authorized with standard auth");
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_WithDomains_Authorized() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(datasetUrn, Set.of(domainUrn));

    // Mock authorization to allow
    when(mockSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp),
            domainsByEntity);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "Should be authorized with domain auth");
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_WithDomains_DeniedScenario() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domainUrn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(datasetUrn, Set.of(domainUrn));

    // Mock authorization to deny
    when(mockSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, "Denied"));

    // Execute - should handle authorization check gracefully
    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp),
            domainsByEntity);

    // Verify method completes without exception
    assertNotNull(results);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_MixedEntities() throws Exception {
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

    // Mock authorization to allow
    when(mockSession.authorize(anyString(), any(), any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp1, mcp2),
            domainsByEntity);

    assertNotNull(results);
    assertEquals(results.size(), 2);
    assertTrue(results.get(mcp1), "Dataset with domain should be authorized");
    assertTrue(results.get(mcp2), "Dataset without domain should be authorized");
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_DifferentOperations() throws Exception {
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

    // Mock authorization to allow
    when(mockSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(createMcp, updateMcp, deleteMcp),
            domainsByEntity);

    assertNotNull(results);
    assertEquals(results.size(), 3);
    assertTrue(results.get(createMcp), "CREATE should be authorized");
    assertTrue(results.get(updateMcp), "UPDATE should be authorized");
    assertTrue(results.get(deleteMcp), "DELETE should be authorized");
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_MultipleDomains() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);
    Urn domain1Urn = Urn.createFromString(DOMAIN_URN_STRING);
    Urn domain2Urn = Urn.createFromString(DOMAIN_URN_STRING_2);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(datasetUrn, Set.of(domain1Urn, domain2Urn));

    // Mock authorization to allow
    when(mockSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp),
            domainsByEntity);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "Should be authorized with multiple domains");
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_EmptyMCPList() {
    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            Collections.emptyList(),
            null);

    assertNotNull(results);
    assertTrue(results.isEmpty());
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_NullURN_ExtractedFromProposal() throws Exception {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(null); // Force URN extraction path
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);

    // Mock EntityKeyUtils to return a URN
    Urn expectedUrn = Urn.createFromString(DATASET_URN_STRING);
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);

    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp),
            null);

    assertNotNull(results);
    assertEquals(results.size(), 1);
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_RestateChangeType() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.RESTATE); // Test RESTATE path

    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp),
            null);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "RESTATE should map to UPDATE operation and be authorized");
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_PatchChangeType() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.PATCH); // Test PATCH path

    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp),
            null);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "PATCH should map to UPDATE operation and be authorized");
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_CreateEntityChangeType() throws Exception {
    Urn datasetUrn = Urn.createFromString(DATASET_URN_STRING);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(datasetUrn);
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.CREATE_ENTITY); // Test CREATE_ENTITY path

    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp),
            null);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.get(mcp), "CREATE_ENTITY should map to CREATE operation and be authorized");
  }

  @Test
  public void testIsAPIAuthorizedMCPsWithDomains_MixedAuthorizationResults() throws Exception {
    Urn dataset1Urn = Urn.createFromString(DATASET_URN_STRING);
    Urn dataset2Urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test2,PROD)");
    Urn domain1Urn = Urn.createFromString(DOMAIN_URN_STRING);

    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setEntityUrn(dataset1Urn);
    mcp1.setEntityType("dataset");
    mcp1.setChangeType(ChangeType.UPSERT);

    MetadataChangeProposal mcp2 = new MetadataChangeProposal();
    mcp2.setEntityUrn(dataset2Urn);
    mcp2.setEntityType("dataset");
    mcp2.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> domainsByEntity = new HashMap<>();
    domainsByEntity.put(dataset1Urn, Set.of(domain1Urn));
    domainsByEntity.put(dataset2Urn, Set.of(domain1Urn));

    // Mock: First entity authorized, second denied
    when(mockSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));

    Map<MetadataChangeProposal, Boolean> results =
        AuthUtil.isAPIAuthorizedMCPsWithDomains(
            mockSession,
            com.linkedin.metadata.authorization.ApiGroup.ENTITY,
            mockEntityRegistry,
            List.of(mcp1, mcp2),
            domainsByEntity);

    assertNotNull(results);
    assertEquals(results.size(), 2);
    assertTrue(results.get(mcp1), "First MCP should be authorized");
    assertFalse(results.get(mcp2), "Second MCP should be denied");
  }
}
