package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.DomainWriteAuthorizationUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainWriteAuthorizationValidatorTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
  private static final Urn DOMAIN_X = UrnUtils.getUrn("urn:li:domain:engineering");
  private static final Urn DOMAIN_Y = UrnUtils.getUrn("urn:li:domain:marketing");

  private MockedStatic<DomainWriteAuthorizationUtils> domainWriteUtilsMock;
  private AspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;
  private AuthorizationSession session;
  private DomainWriteAuthorizationValidator validator;
  private com.linkedin.metadata.models.EntitySpec entitySpec;
  private AspectSpec domainsAspectSpec;

  @BeforeMethod
  public void setUp() {
    domainWriteUtilsMock = Mockito.mockStatic(DomainWriteAuthorizationUtils.class);
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.resolveEntityExists(any(), any(), any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.resolveDomainsAspectExists(any(), any(), any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.extractProposedDomainsByUrn(any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.extractProposedDomainsFromItem(any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(
            () -> DomainWriteAuthorizationUtils.resolveProposedDomainsForItem(any(), any(), any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.resolveAndAccumulateProposedDomains(
                    any(), any(), any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.resolveAndAccumulateProposedDomains(
                    any(), any(), any(), any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(
                    anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.resolveApiOperation(any(), anyBoolean()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.hasDomainMembership(any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.loadPersistedDomains(any(), any(), any()))
        .thenReturn(Map.of());

    aspectRetriever = mock(AspectRetriever.class);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(aspectRetriever);
    session = mock(AuthorizationSession.class);

    entitySpec = mock(com.linkedin.metadata.models.EntitySpec.class);
    when(entitySpec.getName()).thenReturn("dataset");
    domainsAspectSpec = mock(AspectSpec.class);
    when(domainsAspectSpec.getName()).thenReturn("domains");

    validator = new DomainWriteAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(DomainWriteAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(
                List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE", "PATCH"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("ALL")
                        .aspectName("domains")
                        .build()))
            .build());
  }

  @AfterMethod
  public void tearDown() {
    domainWriteUtilsMock.close();
  }

  @Test
  public void testAllowsCreateEntityWithProposedDomain() {
    TestMCP item = domainsItem(ChangeType.CREATE_ENTITY, DOMAIN_X);
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, false));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of()));
    Domains expected = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                    eq(session), eq(DATASET_URN), eq(ApiOperation.CREATE), eq(true), eq(expected)))
        .thenReturn(true);

    assertEquals(validate(item).count(), 0);
    domainWriteUtilsMock.verify(
        () ->
            DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                eq(session), eq(DATASET_URN), eq(ApiOperation.CREATE), eq(true), eq(expected)));
  }

  @Test
  public void testDeniesCreateEntityWhenUnauthorized() {
    TestMCP item = domainsItem(ChangeType.CREATE_ENTITY, DOMAIN_X);
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, false));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of()));
    stubWriteAuth(false);

    assertEquals(validate(item).count(), 1);
  }

  @Test
  public void testAllowsAspectCreateWhenDomainsMissing() {
    TestMCP item = domainsItem(ChangeType.CREATE, DOMAIN_X);
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, true));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of()));
    stubWriteAuth(true);

    assertEquals(validate(item).count(), 0);
    domainWriteUtilsMock.verify(
        () ->
            DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                eq(session), eq(DATASET_URN), eq(ApiOperation.UPDATE), eq(true), any()));
  }

  @Test
  public void testAuthorizesEachDomainsItemIndependently() {
    TestMCP denyItem = domainsItem(ChangeType.UPSERT, DOMAIN_Y);
    TestMCP allowItem = domainsItem(ChangeType.UPSERT, DOMAIN_X);
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, false));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of()));

    Domains denyDomains = new Domains().setDomains(new UrnArray(List.of(DOMAIN_Y)));
    Domains allowDomains = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                    eq(session),
                    eq(DATASET_URN),
                    eq(ApiOperation.CREATE),
                    eq(true),
                    eq(denyDomains)))
        .thenReturn(false);
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                    eq(session),
                    eq(DATASET_URN),
                    eq(ApiOperation.CREATE),
                    eq(true),
                    eq(allowDomains)))
        .thenReturn(true);

    assertEquals(
        validator
            .validateProposedAspectsWithAuth(
                OperationFingerprint.EMPTY, List.of(denyItem, allowItem), retrieverContext, session)
            .count(),
        1);
  }

  @Test
  public void testPatchWithoutResolvableDomainsFailsClosed() {
    TestMCP patchItem =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(domainsAspectSpec)
            .changeType(ChangeType.PATCH)
            .build();
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, true));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of()));
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.resolveAndAccumulateProposedDomains(
                    eq(patchItem), eq(aspectRetriever), any(), any()))
        .thenReturn(null);

    assertEquals(validate(patchItem).count(), 1);
  }

  @Test
  public void testPatchUsesBeforeAfterEditHelper() {
    TestMCP patchItem =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(domainsAspectSpec)
            .changeType(ChangeType.PATCH)
            .build();
    Domains before = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    Domains after = new Domains().setDomains(new UrnArray(List.of(DOMAIN_Y)));
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, true));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(
            Map.of(DATASET_URN, Map.of("domains", new com.linkedin.entity.Aspect(before.data()))));
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.loadPersistedDomains(any(), any(), any()))
        .thenReturn(Map.of(DATASET_URN, before));
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.resolveAndAccumulateProposedDomains(
                    eq(patchItem), eq(aspectRetriever), any(), eq(before)))
        .thenReturn(after);
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(
                    eq(session), eq(DATASET_URN), eq(before), eq(after)))
        .thenReturn(true);

    assertEquals(validate(patchItem).count(), 0);
    domainWriteUtilsMock.verify(
        () ->
            DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(
                eq(session), eq(DATASET_URN), eq(before), eq(after)));
  }

  @Test
  public void testUpsertThenPatchUsesAccumulatedProposedDomains() {
    TestMCP upsert = domainsItem(ChangeType.UPSERT, DOMAIN_X);
    TestMCP patchItem =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(domainsAspectSpec)
            .changeType(ChangeType.PATCH)
            .build();
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, false));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of()));

    Domains upsertDomains = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    Domains patchDomains = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X, DOMAIN_Y)));
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.resolveAndAccumulateProposedDomains(
                    eq(upsert), eq(aspectRetriever), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Map<Urn, Domains> proposedSoFar = invocation.getArgument(2);
              proposedSoFar.put(DATASET_URN, upsertDomains);
              return upsertDomains;
            });
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.resolveAndAccumulateProposedDomains(
                    eq(patchItem), eq(aspectRetriever), any(), any()))
        .thenReturn(patchDomains);
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                    eq(session),
                    eq(DATASET_URN),
                    eq(ApiOperation.CREATE),
                    eq(true),
                    eq(upsertDomains)))
        .thenReturn(true);
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(
                    eq(session), eq(DATASET_URN), eq(upsertDomains), eq(patchDomains)))
        .thenReturn(true);

    assertEquals(
        validator
            .validateProposedAspectsWithAuth(
                OperationFingerprint.EMPTY, List.of(upsert, patchItem), retrieverContext, session)
            .count(),
        0);
    domainWriteUtilsMock.verify(
        () ->
            DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(
                eq(session), eq(DATASET_URN), eq(upsertDomains), eq(patchDomains)));
  }

  @Test
  public void testPatchWithBeforeButUnresolvableAfterFailsClosed() {
    TestMCP patchItem =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(domainsAspectSpec)
            .changeType(ChangeType.PATCH)
            .build();
    Domains before = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, true));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(
            Map.of(DATASET_URN, Map.of("domains", new com.linkedin.entity.Aspect(before.data()))));
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.loadPersistedDomains(any(), any(), any()))
        .thenReturn(Map.of(DATASET_URN, before));
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.resolveAndAccumulateProposedDomains(
                    eq(patchItem), eq(aspectRetriever), any(), eq(before)))
        .thenReturn(null);

    assertEquals(validate(patchItem).count(), 1);
    domainWriteUtilsMock.verify(
        () -> DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(any(), any(), any(), any()),
        Mockito.never());
  }

  @Test
  public void testPreCommitCreateUsesCreatePrivilegeNotEditOnly() {
    ChangeMCP change = mock(ChangeMCP.class);
    when(change.getAspectName()).thenReturn("domains");
    when(change.getUrn()).thenReturn(DATASET_URN);
    when(change.getPreviousAspect(Domains.class)).thenReturn(null);
    Domains after = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    when(change.getAspect(Domains.class)).thenReturn(after);
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, false));
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                    eq(session), eq(DATASET_URN), eq(ApiOperation.CREATE), eq(true), eq(after)))
        .thenReturn(true);

    assertEquals(
        validator
            .validatePreCommitAspectsWithAuth(
                OperationFingerprint.EMPTY, List.of(change), retrieverContext, session)
            .count(),
        0);
    domainWriteUtilsMock.verify(
        () ->
            DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                eq(session), eq(DATASET_URN), eq(ApiOperation.CREATE), eq(true), eq(after)));
    domainWriteUtilsMock.verify(
        () -> DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(any(), any(), any(), any()),
        Mockito.never());
  }

  @Test
  public void testPreCommitExistingDomainsUsesBeforeAfterEdit() {
    ChangeMCP change = mock(ChangeMCP.class);
    when(change.getAspectName()).thenReturn("domains");
    when(change.getUrn()).thenReturn(DATASET_URN);
    Domains before = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    Domains after = new Domains().setDomains(new UrnArray(List.of(DOMAIN_Y)));
    when(change.getPreviousAspect(Domains.class)).thenReturn(before);
    when(change.getAspect(Domains.class)).thenReturn(after);
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, true));
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(
                    eq(session), eq(DATASET_URN), eq(before), eq(after)))
        .thenReturn(true);

    assertEquals(
        validator
            .validatePreCommitAspectsWithAuth(
                OperationFingerprint.EMPTY, List.of(change), retrieverContext, session)
            .count(),
        0);
  }

  @Test
  public void testPreCommitSkipsWhenSessionNull() {
    ChangeMCP change = mock(ChangeMCP.class);
    when(change.getAspectName()).thenReturn("domains");
    Stream<?> results =
        validator.validatePreCommitAspectsWithAuth(
            OperationFingerprint.EMPTY, List.of(change), retrieverContext, null);
    assertEquals(results.count(), 0);
  }

  @Test
  public void testShouldSkipUserDomainAuth_nullSession() {
    assertTrue(DomainWriteAuthorizationValidator.shouldSkipUserDomainAuth(null));
  }

  @Test
  public void testIgnoresNonDomainsAspects() {
    AspectSpec ownershipSpec = mock(AspectSpec.class);
    when(ownershipSpec.getName()).thenReturn("ownership");
    TestMCP ownership =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(ownershipSpec)
            .changeType(ChangeType.CREATE_ENTITY)
            .build();

    assertEquals(validate(ownership).count(), 0);
  }

  private void stubWriteAuth(boolean allowed) {
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                    any(), any(), any(), anyBoolean(), any()))
        .thenReturn(allowed);
  }

  private TestMCP domainsItem(ChangeType changeType, Urn domainUrn) {
    Domains domains = new Domains().setDomains(new UrnArray(List.of(domainUrn)));
    return TestMCP.builder()
        .urn(DATASET_URN)
        .entitySpec(entitySpec)
        .aspectSpec(domainsAspectSpec)
        .recordTemplate(domains)
        .changeType(changeType)
        .build();
  }

  private java.util.stream.Stream<
          com.linkedin.metadata.aspect.plugins.validation.AspectValidationException>
      validate(TestMCP item) {
    return validator.validateProposedAspectsWithAuth(
        OperationFingerprint.EMPTY, List.of(item), retrieverContext, session);
  }
}
