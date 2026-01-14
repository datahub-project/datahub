package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainBasedAuthorizationValidatorTest {

  private static final OperationContext TEST_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(DomainBasedAuthorizationValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "UPDATE", "DELETE"))
          .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
          .build();

  @Mock private ChangeMCP mockChangeMCP;
  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;
  @Mock private AuthorizationSession mockAuthSession;

  private DomainBasedAuthorizationValidator validator;
  private MockedStatic<AuthUtil> authUtilMock;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    validator = new DomainBasedAuthorizationValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    authUtilMock = mockStatic(AuthUtil.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    // Default: mock auth session to be present
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(mockAuthSession);
  }

  @AfterMethod
  public void teardown() {
    authUtilMock.close();
  }

  @Test
  public void testValidatePreCommitAspects_DeleteOperation_ShouldSkip() {
    // Arrange
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.DELETE);

    // Act
    Stream<AspectValidationException> result =
        validator.validatePreCommitAspects(
            Collections.singletonList(mockChangeMCP), mockRetrieverContext);

    // Assert
    assertEquals(result.collect(Collectors.toList()).size(), 0, "DELETE operations should skip validation");
    // Verify no authorization checks were performed
    authUtilMock.verify(
        () -> AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(any(), any(), anyCollection(), anyCollection()),
        never());
  }

  @Test
  public void testValidatePreCommitAspects_NoAuthSession_ShouldFail() {
    // Arrange
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.CREATE);
    // Override default mock to return null for this test
    when(mockRetrieverContext.getAuthorizationSession()).thenReturn(null);

    // Act
    Stream<AspectValidationException> result =
        validator.validatePreCommitAspects(
            Collections.singletonList(mockChangeMCP), mockRetrieverContext);

    // Assert
    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertEquals(exceptions.size(), 1, "Should return validation exception");
    assertTrue(
        exceptions.get(0).getMessage().contains("No authentication details found"),
        "Exception should mention missing authentication");
  }

  @Test
  public void testValidatePreCommitAspects_CreateWithDomain_Authorized() {
    // Arrange
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:engineering");

    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.CREATE);
    when(mockChangeMCP.getAspectName()).thenReturn(DOMAINS_ASPECT_NAME);

    // Mock domain aspect
    Domains domainsAspect = new Domains();
    domainsAspect.setDomains(Collections.singletonList(domainUrn).stream()
        .collect(Collectors.toCollection(UrnArray::new)));
    Aspect domainAspectData = mock(Aspect.class);
    when(domainAspectData.data()).thenReturn(domainsAspect.data());
    when(mockChangeMCP.getAspect(Domains.class)).thenReturn(domainsAspect);

    // Mock authorization - authorized
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            eq(mockAuthSession),
            eq(ApiOperation.CREATE),
            eq(Collections.singletonList(entityUrn)),
            anyCollection()))
        .thenReturn(true);

    // Act
    Stream<AspectValidationException> result =
        validator.validatePreCommitAspects(
            Collections.singletonList(mockChangeMCP), mockRetrieverContext);

    // Assert
    assertEquals(result.collect(Collectors.toList()).size(), 0, "Should pass validation when authorized");
  }

  @Test
  public void testValidatePreCommitAspects_CreateWithDomain_Unauthorized() {
    // Arrange
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:engineering");

    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.CREATE);
    when(mockChangeMCP.getAspectName()).thenReturn(DOMAINS_ASPECT_NAME);

    // Mock domain aspect
    Domains domainsAspect = new Domains();
    domainsAspect.setDomains(Collections.singletonList(domainUrn).stream()
        .collect(Collectors.toCollection(UrnArray::new)));
    when(mockChangeMCP.getAspect(Domains.class)).thenReturn(domainsAspect);

    // Mock authorization - unauthorized
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            eq(mockAuthSession),
            eq(ApiOperation.CREATE),
            eq(Collections.singletonList(entityUrn)),
            anyCollection()))
        .thenReturn(false);

    // Act
    Stream<AspectValidationException> result =
        validator.validatePreCommitAspects(
            Collections.singletonList(mockChangeMCP), mockRetrieverContext);

    // Assert
    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertEquals(exceptions.size(), 1, "Should return validation exception when unauthorized");
    assertTrue(
        exceptions.get(0).getMessage().contains("Unauthorized"),
        "Exception should mention unauthorized");
  }

  @Test
  public void testValidatePreCommitAspects_UpdateNonDomainAspect_FetchesExistingDomains() {
    // Arrange
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:engineering");

    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.UPDATE);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    // Mock existing domain aspect
    Domains existingDomains = new Domains();
    existingDomains.setDomains(Collections.singletonList(domainUrn).stream()
        .collect(Collectors.toCollection(UrnArray::new)));
    Aspect domainAspectData = mock(Aspect.class);
    when(domainAspectData.data()).thenReturn(existingDomains.data());
    when(mockAspectRetriever.getLatestAspectObject(entityUrn, DOMAINS_ASPECT_NAME))
        .thenReturn(domainAspectData);

    // Mock authorization - authorized
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            eq(mockAuthSession),
            eq(ApiOperation.UPDATE),
            eq(Collections.singletonList(entityUrn)),
            anyCollection()))
        .thenReturn(true);

    // Act
    Stream<AspectValidationException> result =
        validator.validatePreCommitAspects(
            Collections.singletonList(mockChangeMCP), mockRetrieverContext);

    // Assert
    assertEquals(result.collect(Collectors.toList()).size(), 0, "Should pass validation");
    // Verify existing domains were fetched
    verify(mockAspectRetriever).getLatestAspectObject(entityUrn, DOMAINS_ASPECT_NAME);
  }

  @Test
  public void testValidatePreCommitAspects_NoDomains_ShouldAllow() {
    // Arrange
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");

    when(mockChangeMCP.getUrn()).thenReturn(entityUrn);
    when(mockChangeMCP.getChangeType()).thenReturn(ChangeType.CREATE);
    when(mockChangeMCP.getAspectName()).thenReturn("datasetProperties");

    // No domains aspect
    when(mockAspectRetriever.getLatestAspectObject(entityUrn, DOMAINS_ASPECT_NAME))
        .thenReturn(null);

    // Mock authorization with empty domains - authorized
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            eq(mockAuthSession),
            eq(ApiOperation.CREATE),
            eq(Collections.singletonList(entityUrn)),
            eq(Collections.emptySet())))
        .thenReturn(true);

    // Act
    Stream<AspectValidationException> result =
        validator.validatePreCommitAspects(
            Collections.singletonList(mockChangeMCP), mockRetrieverContext);

    // Assert
    assertEquals(result.collect(Collectors.toList()).size(), 0, "Should pass validation when no domains");
  }
}