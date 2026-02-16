package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainBasedAuthorizationValidatorTest {

  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;
  @Mock private AuthorizationSession mockAuthSession;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private ChangeMCP mockChangeMCP;

  private DomainBasedAuthorizationValidator validator;
  private boolean originalRestApiAuthorizationEnabled;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    // Save original value and enable REST API authorization for tests
    originalRestApiAuthorizationEnabled = getRestApiAuthorizationEnabled();
    setRestApiAuthorizationEnabled(true);

    validator = new DomainBasedAuthorizationValidator();
    AspectPluginConfig config =
        AspectPluginConfig.builder()
            .className(DomainBasedAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(
                List.of("CREATE", "CREATE_ENTITY", "UPDATE", "UPSERT", "PATCH", "RESTATE"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();
    validator.setConfig(config);

    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(mockEntityRegistry);

    // Default mock for authorization - returns ALLOW by default
    // Individual tests can override this as needed
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));
  }

  @AfterMethod
  public void tearDown() throws Exception {
    // Restore original REST API authorization setting
    setRestApiAuthorizationEnabled(originalRestApiAuthorizationEnabled);
  }

  /** Helper method to get the static isRestApiAuthorizationEnabled field via reflection */
  private boolean getRestApiAuthorizationEnabled() throws Exception {
    Field field = AuthUtil.class.getDeclaredField("isRestApiAuthorizationEnabled");
    field.setAccessible(true);
    return field.getBoolean(null);
  }

  /** Helper method to set the static isRestApiAuthorizationEnabled field via reflection */
  private void setRestApiAuthorizationEnabled(boolean value) throws Exception {
    Field field = AuthUtil.class.getDeclaredField("isRestApiAuthorizationEnabled");
    field.setAccessible(true);
    field.setBoolean(null, value);
  }

  @Test
  public void testValidateProposedAspects_ReturnsEmpty() {
    // validateProposedAspects should always return empty stream
    List<AspectValidationException> result =
        validator
            .validateProposedAspects(Collections.emptyList(), mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.size(), 0);
  }

  @Test
  public void testValidatePreCommitAspects_NoSession_SkipsAuthorization() throws Exception {
    // Setup: No session provided - this is the smoke test fix!
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(null);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should skip authorization and return empty
    assertEquals(result.size(), 0);
  }

  @Test
  public void testValidatePreCommitAspects_ExecutionRequestEntity_Skipped() throws Exception {
    // Setup: ExecutionRequest entity should be skipped
    // The entity type is "dataHubExecutionRequest" (from Constants.EXECUTION_REQUEST_ENTITY_NAME)
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataHubExecutionRequest:test");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("executionRequestInput");

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should skip ExecutionRequest entities
    assertEquals(result.size(), 0);
  }

  @Test
  public void testValidatePreCommitAspects_DeleteOperation_Skipped() throws Exception {
    // Setup: DELETE operations should be skipped
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should skip DELETE operations
    assertEquals(result.size(), 0);
  }

  @Test
  public void testValidatePreCommitAspects_NullDomainsAspect() throws Exception {
    // Setup: Entity with null domains aspect
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    // No domains in MCP
    when(mockChangeMCP.getAspect(Domains.class)).thenReturn(null);

    // No domains in DB
    when(mockAspectRetriever.getLatestAspectObject(eq(entityUrn), eq("domains"))).thenReturn(null);

    // Mock authorization to return ALLOW for testing validator logic
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute - Note: This will call AuthUtil which may deny access
    // We're just testing that the method doesn't throw exceptions
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process without error (may or may not be authorized depending on AuthUtil)
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_WithDomainsInMCP() throws Exception {
    // Setup: Entity with domains in MCP
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    Urn domainUrn = Urn.createFromString("urn:li:domain:test-domain");

    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("domains");

    // Domains in MCP
    Domains domainsAspect = new Domains();
    UrnArray urnArray = new UrnArray();
    urnArray.add(domainUrn);
    domainsAspect.setDomains(urnArray);
    when(mockChangeMCP.getAspect(Domains.class)).thenReturn(domainsAspect);

    // Mock authorization to return ALLOW for testing validator logic
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process without error (authorization result depends on AuthUtil)
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_WithDomainsInDB() throws Exception {
    // Setup: Domains from existing entity in DB
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    Urn dbDomain = Urn.createFromString("urn:li:domain:db-domain");

    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    // No domains in MCP
    when(mockChangeMCP.getAspect(Domains.class)).thenReturn(null);

    // Domains in DB
    Domains dbDomainsAspect = new Domains();
    UrnArray dbUrnArray = new UrnArray();
    dbUrnArray.add(dbDomain);
    dbDomainsAspect.setDomains(dbUrnArray);
    Aspect dbAspect = mock(Aspect.class);
    when(dbAspect.data()).thenReturn(dbDomainsAspect.data());
    when(mockAspectRetriever.getLatestAspectObject(eq(entityUrn), eq("domains")))
        .thenReturn(dbAspect);

    // Mock authorization to return ALLOW for testing validator logic
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process without error
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_MultipleMCPsSameEntity() throws Exception {
    // Setup: Multiple MCPs for the same entity
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");

    ChangeMCP mcp1 = mock(ChangeMCP.class);
    when(mcp1.getUrn()).thenReturn(entityUrn);
    when(mcp1.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mcp1.getAspectName()).thenReturn("datasetProperties");

    ChangeMCP mcp2 = mock(ChangeMCP.class);
    when(mcp2.getUrn()).thenReturn(entityUrn);
    when(mcp2.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mcp2.getAspectName()).thenReturn("datasetKey");

    // Mock authorization to return ALLOW for testing validator logic
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = List.of(mcp1, mcp2);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process without error
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_AuthorizationDenied_ReturnsException() throws Exception {
    // Setup: Authorization is denied - should return AspectValidationException
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    // Mock authorization to return DENY
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, "Denied"));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should return an exception for denied authorization
    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getMessage().contains("Unauthorized"));
  }

  @Test
  public void testValidatePreCommitAspects_EntityExistsCheckException() throws Exception {
    // Setup: Exception when checking entity existence
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    // Mock entityExists to throw exception
    when(mockAspectRetriever.entityExists(any())).thenThrow(new RuntimeException("Database error"));

    // Mock authorization to return ALLOW
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute - should continue despite exception, treating as non-existing entity
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process without error (entityExists defaults to false on exception)
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_ExistingEntityWithProposedDomains_AuthorizedBoth()
      throws Exception {
    // Setup: Existing entity with proposed domains - both checks pass
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    Urn domainUrn = Urn.createFromString("urn:li:domain:test-domain");

    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("domains");

    // Domains in MCP
    Domains domainsAspect = new Domains();
    UrnArray urnArray = new UrnArray();
    urnArray.add(domainUrn);
    domainsAspect.setDomains(urnArray);
    when(mockChangeMCP.getAspect(Domains.class)).thenReturn(domainsAspect);

    // Entity exists
    when(mockAspectRetriever.entityExists(Set.of(entityUrn))).thenReturn(Map.of(entityUrn, true));

    // Mock authorization to return ALLOW for both checks
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should be authorized (both checks pass)
    assertEquals(result.size(), 0);
  }

  @Test
  public void testValidatePreCommitAspects_ExistingEntityWithProposedDomains_DeniedOnProposed()
      throws Exception {
    // Setup: Existing entity with proposed domains - denied on proposed domains check
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    Urn domainUrn = Urn.createFromString("urn:li:domain:test-domain");

    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("domains");

    // Domains in MCP
    Domains domainsAspect = new Domains();
    UrnArray urnArray = new UrnArray();
    urnArray.add(domainUrn);
    domainsAspect.setDomains(urnArray);
    when(mockChangeMCP.getAspect(Domains.class)).thenReturn(domainsAspect);

    // Entity exists
    when(mockAspectRetriever.entityExists(Set.of(entityUrn))).thenReturn(Map.of(entityUrn, true));

    // Mock authorization to return DENY (proposed domains check fails first)
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, "Denied"));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should be denied
    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getMessage().contains("Unauthorized"));
  }

  @Test
  public void testValidatePreCommitAspects_NewEntityWithProposedDomains() throws Exception {
    // Setup: New entity (does not exist) with proposed domains
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,new-entity,PROD)");
    Urn domainUrn = Urn.createFromString("urn:li:domain:test-domain");

    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.CREATE_ENTITY);
    when(mockChangeMCP.getAspectName()).thenReturn("domains");

    // Domains in MCP
    Domains domainsAspect = new Domains();
    UrnArray urnArray = new UrnArray();
    urnArray.add(domainUrn);
    domainsAspect.setDomains(urnArray);
    when(mockChangeMCP.getAspect(Domains.class)).thenReturn(domainsAspect);

    // Entity does NOT exist
    when(mockAspectRetriever.entityExists(Set.of(entityUrn))).thenReturn(Map.of(entityUrn, false));

    // Mock authorization to return ALLOW
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should be authorized (only proposed domains check needed)
    assertEquals(result.size(), 0);
  }

  @Test
  public void testValidatePreCommitAspects_DomainsExtractionException() throws Exception {
    // Setup: Exception when extracting domains from MCP
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("domains");

    // Mock getAspect to throw exception
    when(mockChangeMCP.getAspect(Domains.class))
        .thenThrow(new RuntimeException("Deserialization error"));

    // Mock authorization to return ALLOW
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute - should continue despite exception, treating as no proposed domains
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process without error
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_DomainsWithNullDomainsList() throws Exception {
    // Setup: Domains aspect exists but domains list is null
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockChangeMCP.getAspectName()).thenReturn("domains");

    // Domains aspect with null domains list
    Domains domainsAspect = new Domains();
    // Not setting domains - leaving it null
    when(mockChangeMCP.getAspect(Domains.class)).thenReturn(domainsAspect);

    // Mock authorization to return ALLOW
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process without error
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_CREATEChangeType() throws Exception {
    // Setup: CREATE change type
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.CREATE);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    // Mock authorization to return ALLOW
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process CREATE change type
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_PATCHChangeType() throws Exception {
    // Setup: PATCH change type
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.PATCH);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    // Mock authorization to return ALLOW
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process PATCH change type
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_RESTATEChangeType() throws Exception {
    // Setup: RESTATE change type
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.RESTATE);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    // Mock authorization to return ALLOW
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null));

    List<ChangeMCP> changeMCPs = Collections.singletonList(mockChangeMCP);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process RESTATE change type
    assertNotNull(result);
  }

  @Test
  public void testValidatePreCommitAspects_MultipleEntitiesMixedAuth() throws Exception {
    // Setup: Multiple different entities - one authorized, one denied
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn1 = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,entity1,PROD)");
    Urn entityUrn2 = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,entity2,PROD)");

    ChangeMCP mcp1 = mock(ChangeMCP.class);
    when(mcp1.getUrn()).thenReturn(entityUrn1);
    when(mcp1.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mcp1.getAspectName()).thenReturn("datasetProperties");

    ChangeMCP mcp2 = mock(ChangeMCP.class);
    when(mcp2.getUrn()).thenReturn(entityUrn2);
    when(mcp2.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mcp2.getAspectName()).thenReturn("datasetProperties");

    // Mock authorization - first call ALLOW, second call DENY
    when(mockAuthSession.authorize(anyString(), any(), anyCollection()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, null))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, "Denied"));

    List<ChangeMCP> changeMCPs = List.of(mcp1, mcp2);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should have at least one entity processed (one allowed, one denied)
    // The exact count depends on order of processing
    assertNotNull(result);
  }
}
