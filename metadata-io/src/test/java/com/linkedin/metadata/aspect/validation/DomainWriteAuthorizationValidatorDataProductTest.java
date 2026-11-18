package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
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
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * {@link DomainWriteAuthorizationValidator} against the real {@code DomainWriteAuthorizationUtils}
 * for the data product shapes: the {@code createDataProduct} first-domains write, and a move.
 */
public class DomainWriteAuthorizationValidatorDataProductTest {

  private static final Urn DATA_PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:auth-test");
  private static final Urn DOMAIN_X = UrnUtils.getUrn("urn:li:domain:engineering");
  private static final Urn DOMAIN_Y = UrnUtils.getUrn("urn:li:domain:marketing");

  private AspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;
  private DomainWriteAuthorizationValidator validator;
  private com.linkedin.metadata.models.EntitySpec entitySpec;
  private AspectSpec domainsAspectSpec;

  @BeforeMethod
  public void setUp() {
    aspectRetriever = mock(AspectRetriever.class);
    // Every referenced entity (the product and the domains) exists.
    when(aspectRetriever.entityExists(any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              return urns.stream().collect(Collectors.toMap(Function.identity(), urn -> true));
            });
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(aspectRetriever);

    entitySpec = mock(com.linkedin.metadata.models.EntitySpec.class);
    when(entitySpec.getName()).thenReturn("dataProduct");
    domainsAspectSpec = mock(AspectSpec.class);
    when(domainsAspectSpec.getName()).thenReturn(DOMAINS_ASPECT_NAME);

    validator = new DomainWriteAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(DomainWriteAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "PATCH"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("ALL")
                        .aspectName(DOMAINS_ASPECT_NAME)
                        .build()))
            .build());
  }

  @Test
  public void testFirstDomainsWriteAllowedWithManageDataProductsOnDomain() {
    // createDataProduct: the product exists (dataProductProperties was just written) but has no
    // domains aspect yet, and setDomain upserts its first domain.
    mockPersistedDomains(null);

    assertEquals(validate(domainsItem(ChangeType.UPSERT, DOMAIN_X), managerOf(DOMAIN_X)).size(), 0);
  }

  @Test
  public void testFirstDomainsWriteDeniedOnUnmanagedDomain() {
    mockPersistedDomains(null);

    assertEquals(validate(domainsItem(ChangeType.UPSERT, DOMAIN_Y), managerOf(DOMAIN_X)).size(), 1);
  }

  @Test
  public void testMoveIsNotAuthorizedByManageDataProducts() {
    // A product that already has domains is being moved: Edit Entity / Edit Domain on the product
    // is required, even for a manager of both domains.
    mockPersistedDomains(domains(DOMAIN_X));

    assertEquals(
        validate(domainsItem(ChangeType.UPSERT, DOMAIN_Y), managerOf(DOMAIN_X, DOMAIN_Y)).size(),
        1);
  }

  @Test
  public void testPreCommitFirstDomainsAllowedAndMoveDenied() {
    AuthorizationSession managerOfBoth = managerOf(DOMAIN_X, DOMAIN_Y);

    assertEquals(
        validator
            .validatePreCommitAspectsWithAuth(
                OperationFingerprint.EMPTY,
                List.of(changeMCP(null, domains(DOMAIN_X))),
                retrieverContext,
                managerOfBoth)
            .count(),
        0);
    assertEquals(
        validator
            .validatePreCommitAspectsWithAuth(
                OperationFingerprint.EMPTY,
                List.of(changeMCP(domains(DOMAIN_X), domains(DOMAIN_Y))),
                retrieverContext,
                managerOfBoth)
            .count(),
        1);
  }

  private void mockPersistedDomains(Domains persisted) {
    Map<String, Aspect> aspects =
        persisted == null ? Map.of() : Map.of(DOMAINS_ASPECT_NAME, new Aspect(persisted.data()));
    when(aspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DOMAINS_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, aspects));
  }

  private static Domains domains(Urn domain) {
    return new Domains().setDomains(new UrnArray(List.of(domain)));
  }

  private TestMCP domainsItem(ChangeType changeType, Urn domainUrn) {
    return TestMCP.builder()
        .urn(DATA_PRODUCT_URN)
        .entitySpec(entitySpec)
        .aspectSpec(domainsAspectSpec)
        .recordTemplate(domains(domainUrn))
        .changeType(changeType)
        .build();
  }

  private static ChangeMCP changeMCP(Domains previous, Domains proposed) {
    ChangeMCP change = mock(ChangeMCP.class);
    when(change.getUrn()).thenReturn(DATA_PRODUCT_URN);
    when(change.getAspectName()).thenReturn(DOMAINS_ASPECT_NAME);
    when(change.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(change.getPreviousAspect(Domains.class)).thenReturn(previous);
    when(change.getAspect(Domains.class)).thenReturn(proposed);
    return change;
  }

  private List<?> validate(TestMCP item, AuthorizationSession session) {
    return validator
        .validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY, List.of(item), retrieverContext, session)
        .collect(Collectors.toList());
  }

  /** Grants {@code MANAGE_DATA_PRODUCTS} on the given domains only, and nothing else. */
  private static AuthorizationSession managerOf(Urn... managedDomains) {
    Set<String> managed =
        Set.of(managedDomains).stream().map(Urn::toString).collect(Collectors.toSet());
    AuthorizationSession session = mock(AuthorizationSession.class);
    when(session.authorize(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              String privilege = invocation.getArgument(0);
              EntitySpec resource = invocation.getArgument(1);
              boolean allowed =
                  PoliciesConfig.MANAGE_DATA_PRODUCTS_PRIVILEGE.getType().equals(privilege)
                      && resource != null
                      && managed.contains(resource.getEntity());
              return new AuthorizationResult(
                  null,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  null);
            });
    return session;
  }
}
