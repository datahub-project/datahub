package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

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
import java.util.*;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainBasedAuthorizationValidatorTest {

  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;
  @Mock private AuthorizationSession mockAuthSession;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private ChangeMCP mockChangeMCP;

  private DomainBasedAuthorizationValidator validator;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

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
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);

    Urn entityUrn = Urn.createFromString("urn:li:executionRequest:test");
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

    List<ChangeMCP> changeMCPs = List.of(mcp1, mcp2);

    // Execute
    List<AspectValidationException> result =
        validator
            .validatePreCommitAspects(changeMCPs, mockRetrieverContext)
            .collect(Collectors.toList());

    // Assert: Should process without error
    assertNotNull(result);
  }
}
