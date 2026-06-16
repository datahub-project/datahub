package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.QUERY_SUBJECTS_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainAssociation;
import com.linkedin.domain.DomainAssociationArray;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityAspectAuthorizationUtilsTest {

  private static final Urn DATA_PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:auth-test");
  private static final Urn DOMAIN_A = UrnUtils.getUrn("urn:li:domain:domain-a");
  private static final Urn DOMAIN_B = UrnUtils.getUrn("urn:li:domain:domain-b");
  private static final Urn QUERY_URN = UrnUtils.getUrn("urn:li:query:auth-test");
  private static final Urn SUBJECT_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)");
  private static final Urn ASSET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,asset,PROD)");

  private AuthorizationSession mockAuthSession;
  private AspectRetriever mockAspectRetriever;
  private MockedStatic<AuthUtil> authUtilMockedStatic;

  @BeforeMethod
  public void setup() {
    authUtilMockedStatic = Mockito.mockStatic(AuthUtil.class);
    mockAuthSession = mock(AuthorizationSession.class);
    mockAspectRetriever = mock(AspectRetriever.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMockedStatic.close();
  }

  @Test
  public void testResolveUniqueDomainUrns_prefersDomainAssociations() {
    Domains domains = new Domains();
    DomainAssociation association = new DomainAssociation();
    association.setDomain(DOMAIN_A);
    DomainAssociationArray associations = new DomainAssociationArray();
    associations.add(association);
    domains.setDomainAssociations(associations);
    domains.setDomains(new UrnArray(DOMAIN_B));

    Set<Urn> result = EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(domains);

    Assert.assertEquals(result, Set.of(DOMAIN_A));
  }

  @Test
  public void testResolveUniqueDomainUrns_fallsBackToLegacyDomains() {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(DOMAIN_A, DOMAIN_B));

    Set<Urn> result = EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(domains);

    Assert.assertEquals(result, Set.of(DOMAIN_A, DOMAIN_B));
  }

  @Test
  public void testResolveUniqueDomainUrns_deduplicatesDomains() {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(DOMAIN_A, DOMAIN_A, DOMAIN_B));

    Set<Urn> result = EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(domains);

    Assert.assertEquals(result, Set.of(DOMAIN_A, DOMAIN_B));
  }

  @Test
  public void testResolveUniqueDomainUrns_fromAspect() {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(DOMAIN_A));
    Aspect aspect = new Aspect(domains.data());

    Set<Urn> result = EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(aspect);

    Assert.assertEquals(result, Set.of(DOMAIN_A));
  }

  @Test
  public void testIsAuthorizedToManageDataProductsOnAnyDomain_requiresOneAuthorizedDomain() {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("domain", DOMAIN_A.toString()))))
        .thenReturn(true);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("domain", DOMAIN_B.toString()))))
        .thenReturn(false);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToManageDataProductsOnAnyDomain(
            mockAuthSession, Set.of(DOMAIN_A, DOMAIN_B)));
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.isAuthorizedToManageDataProductsOnAnyDomain(
            mockAuthSession, Set.of(DOMAIN_B)));
  }

  @Test
  public void testIsAuthorizedToManageDataProductsOnAnyDomain_emptyDomainsDenied() {
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.isAuthorizedToManageDataProductsOnAnyDomain(
            mockAuthSession, Set.of()));
  }

  @Test
  public void testIsAuthorizedToChangeDataProductMembership_allowsProductSideCrossDomain() {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("domain", DOMAIN_A.toString()))))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToChangeDataProductMembership(
            mockAuthSession, Set.of(DOMAIN_A, DOMAIN_B), Set.of(ASSET_URN)));
  }

  @Test
  public void testIsAuthorizedToChangeDataProductMembership_allowsAssetSideOnly() {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("dataset", ASSET_URN.toString()))))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToChangeDataProductMembership(
            mockAuthSession, Set.of(), Set.of(ASSET_URN)));
  }

  @Test
  public void testIsAuthorizedToChangeDataProductMembership_deniesWhenNeitherPathSucceeds() {
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.isAuthorizedToChangeDataProductMembership(
            mockAuthSession, Set.of(DOMAIN_A), Set.of(ASSET_URN)));
  }

  @Test
  public void testIsAuthorizedToChangeDataProductMembership_allowsRemoveViaProductSide() {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("domain", DOMAIN_A.toString()))))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToChangeDataProductMembership(
            mockAuthSession, Set.of(DOMAIN_A), Set.of(ASSET_URN)));
  }

  @Test
  public void testFilterUnauthorizedToManageDataProductMembership_usesProposedProductDomains() {
    Domains productDomains = new Domains();
    productDomains.setDomains(new UrnArray(DOMAIN_A));
    Aspect proposedProductDomains = new Aspect(productDomains.data());

    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DOMAINS_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, Map.of()));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("domain", DOMAIN_A.toString()))))
        .thenReturn(true);

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            Map.of(DATA_PRODUCT_URN, Set.of(ASSET_URN)),
            Map.of(DATA_PRODUCT_URN, proposedProductDomains));

    Assert.assertTrue(unauthorized.isEmpty());
  }

  @Test
  public void
      testFilterUnauthorizedToManageDataProductMembership_deniesWithoutProductOrAssetAuth() {
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DOMAINS_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, Map.of()));

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            Map.of(DATA_PRODUCT_URN, Set.of(ASSET_URN)));

    Assert.assertEquals(unauthorized, Set.of(DATA_PRODUCT_URN));
  }

  @Test
  public void
      testFilterUnauthorizedToManageDataProductMembership_allowsAssetSideWithoutProductDomains() {
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DOMAINS_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, Map.of()));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("dataset", ASSET_URN.toString()))))
        .thenReturn(true);

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            Map.of(DATA_PRODUCT_URN, Set.of(ASSET_URN)));

    Assert.assertTrue(unauthorized.isEmpty());
  }

  @Test
  public void testFilterUnauthorizedToManageDataProductMembership_allowsProductManageOnAnyDomain() {
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DOMAINS_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                DATA_PRODUCT_URN, Map.of(DOMAINS_ASPECT_NAME, domainsAspect(DOMAIN_A, DOMAIN_B))));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("domain", DOMAIN_A.toString()))))
        .thenReturn(true);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("domain", DOMAIN_B.toString()))))
        .thenReturn(false);

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            Map.of(DATA_PRODUCT_URN, Set.of(ASSET_URN)));

    Assert.assertTrue(unauthorized.isEmpty());
  }

  private static Aspect domainsAspect(Urn... domainUrns) {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(List.of(domainUrns)));
    return new Aspect(domains.data());
  }

  @Test
  public void testFilterUnauthorizedToEditLogicalParent_emptyMap() {
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.filterUnauthorizedToEditLogicalParent(
                mockAuthSession, Map.of())
            .isEmpty());
  }

  @Test
  public void testFilterUnauthorizedToEditLogicalParent_returnsUnauthorizedChildren() {
    Urn child = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,child,PROD)");
    Urn parent = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,parent,PROD)");

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorizedEntityUrns(
                    eq(mockAuthSession), eq(UPDATE), eq(Set.of(child, parent))))
        .thenReturn(false);

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToEditLogicalParent(
            mockAuthSession, Map.of(child, Set.of(child, parent)));

    Assert.assertEquals(unauthorized, Set.of(child));
  }

  @Test
  public void testFilterUnauthorizedToManageDataProductMembership_emptyChangedAssets() {
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, Map.of())
            .isEmpty());
  }

  @Test
  public void testIsAuthorizedToChangeDataProductMembership_emptyAssetsDenied() {
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.isAuthorizedToChangeDataProductMembership(
            mockAuthSession, Set.of(DOMAIN_A), Set.of()));
  }

  @Test
  public void testFilterViewableQueryEntities_emptyInput() {
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
                OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, List.of())
            .isEmpty());
  }

  @Test
  public void testFilterViewableQueryEntities_deniesQueryWithNoSubjects() {
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of()));

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, List.of(QUERY_URN));

    Assert.assertTrue(viewable.isEmpty());
  }

  @Test
  public void testFilterViewableQueryEntities_allowsViaCanViewEntityOnSubject() {
    QuerySubjects querySubjects = new QuerySubjects();
    QuerySubject subject = new QuerySubject();
    subject.setEntity(SUBJECT_DATASET);
    querySubjects.setSubjects(new QuerySubjectArray(subject));
    Aspect subjectsAspect = new Aspect(querySubjects.data());

    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of(QUERY_SUBJECTS_ASPECT_NAME, subjectsAspect)));

    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(SUBJECT_DATASET)))
        .thenReturn(true);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, List.of(QUERY_URN));

    Assert.assertEquals(viewable, Set.of(QUERY_URN));
  }

  @Test
  public void testCanViewQueryEntity_delegatesToFilterViewableQueryEntities() {
    QuerySubjects querySubjects = new QuerySubjects();
    QuerySubject subject = new QuerySubject();
    subject.setEntity(SUBJECT_DATASET);
    querySubjects.setSubjects(new QuerySubjectArray(subject));
    Aspect subjectsAspect = new Aspect(querySubjects.data());

    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of(QUERY_SUBJECTS_ASPECT_NAME, subjectsAspect)));

    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(SUBJECT_DATASET)))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.canViewQueryEntity(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, QUERY_URN));
  }

  @Test
  public void testIsQueryEntity() {
    Assert.assertTrue(EntityAspectAuthorizationUtils.isQueryEntity(QUERY_URN));
    Assert.assertFalse(EntityAspectAuthorizationUtils.isQueryEntity(ASSET_URN));
  }

  @Test
  public void testResolveUniqueDomainUrns_nullDomains() {
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.resolveUniqueDomainUrns((Domains) null).isEmpty());
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.resolveUniqueDomainUrns((Aspect) null).isEmpty());
  }

  @Test
  public void testFilterViewableQueryEntities_allowsEditQueriesOnSubjectWithoutView() {
    QuerySubjects querySubjects = new QuerySubjects();
    QuerySubject subject = new QuerySubject();
    subject.setEntity(SUBJECT_DATASET);
    querySubjects.setSubjects(new QuerySubjectArray(subject));
    Aspect subjectsAspect = new Aspect(querySubjects.data());

    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of(QUERY_SUBJECTS_ASPECT_NAME, subjectsAspect)));

    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(SUBJECT_DATASET)))
        .thenReturn(false);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("dataset", SUBJECT_DATASET.toString()))))
        .thenReturn(true);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, List.of(QUERY_URN));

    Assert.assertEquals(viewable, Set.of(QUERY_URN));
  }
}
