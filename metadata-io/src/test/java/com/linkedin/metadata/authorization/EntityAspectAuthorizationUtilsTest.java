package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.QUERY_SUBJECTS_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
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
import java.util.Collection;
import java.util.LinkedHashSet;
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
  private static final Urn PHYSICAL_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,physical,PROD)");
  private static final Urn LOGICAL_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:logical,logical,PROD)");
  private static final Urn PHYSICAL_SCHEMA_FIELD =
      UrnUtils.getUrn("urn:li:schemaField:(" + PHYSICAL_DATASET + ",physical_field)");
  private static final Urn LOGICAL_SCHEMA_FIELD =
      UrnUtils.getUrn("urn:li:schemaField:(" + LOGICAL_DATASET + ",logical_field)");

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
  public void testIsAuthorizedToRenameDataProduct_allowsManageOnDomain() {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("domain", DOMAIN_A.toString()))))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToRenameDataProduct(
            mockAuthSession, DATA_PRODUCT_URN, Set.of(DOMAIN_A)));
  }

  @Test
  public void testIsAuthorizedToRenameDataProduct_allowsEditOnProduct() {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("dataProduct", DATA_PRODUCT_URN.toString()))))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToRenameDataProduct(
            mockAuthSession, DATA_PRODUCT_URN, Set.of()));
  }

  @Test
  public void testIsAuthorizedToRenameDataProduct_deniesWithoutPrivilege() {
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.isAuthorizedToRenameDataProduct(
            mockAuthSession, DATA_PRODUCT_URN, Set.of(DOMAIN_A)));
  }

  @Test
  public void testFilterUnauthorizedToRenameDataProduct_returnsEmptyForEmptyInput() {
    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, Set.of(), Map.of());

    Assert.assertTrue(unauthorized.isEmpty());
  }

  @Test
  public void testFilterUnauthorizedToRenameDataProduct_usesProposedProductDomains() {
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
        EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            Set.of(DATA_PRODUCT_URN),
            Map.of(DATA_PRODUCT_URN, proposedProductDomains));

    Assert.assertTrue(unauthorized.isEmpty());
  }

  @Test
  public void testFilterUnauthorizedToRenameDataProduct_usesPersistedDomainsWhenNoProposed() {
    Domains productDomains = new Domains();
    productDomains.setDomains(new UrnArray(DOMAIN_A));
    Aspect persistedDomains = new Aspect(productDomains.data());

    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DOMAINS_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, Map.of(DOMAINS_ASPECT_NAME, persistedDomains)));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("domain", DOMAIN_A.toString()))))
        .thenReturn(true);

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            Set.of(DATA_PRODUCT_URN),
            Map.of());

    Assert.assertTrue(unauthorized.isEmpty());
  }

  @Test
  public void testFilterUnauthorizedToRenameDataProduct_allowsEditOnProductWithoutDomains() {
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DOMAINS_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, Map.of()));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("dataProduct", DATA_PRODUCT_URN.toString()))))
        .thenReturn(true);

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            Set.of(DATA_PRODUCT_URN),
            Map.of());

    Assert.assertTrue(unauthorized.isEmpty());
  }

  @Test
  public void testFilterUnauthorizedToRenameDataProduct_deniesWithoutPrivilege() {
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DOMAINS_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, Map.of()));

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            Set.of(DATA_PRODUCT_URN),
            Map.of());

    Assert.assertEquals(unauthorized, Set.of(DATA_PRODUCT_URN));
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

    mockEditEntityAuthorizedOnUrns(child);

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToEditLogicalParent(
            mockAuthSession, Map.of(child, Set.of(child, parent)));

    Assert.assertEquals(unauthorized, Set.of(child));
  }

  @Test
  public void testResolveLogicalParentAuthorizationCandidates_includesDatasetForSchemaField() {
    LinkedHashSet<Urn> candidates =
        EntityAspectAuthorizationUtils.resolveLogicalParentAuthorizationCandidates(
            PHYSICAL_SCHEMA_FIELD);

    Assert.assertEquals(List.copyOf(candidates), List.of(PHYSICAL_DATASET, PHYSICAL_SCHEMA_FIELD));
  }

  @Test
  public void testResolveLogicalParentAuthorizationCandidates_datasetOnly() {
    Set<Urn> candidates =
        EntityAspectAuthorizationUtils.resolveLogicalParentAuthorizationCandidates(
            PHYSICAL_DATASET);

    Assert.assertEquals(candidates, Set.of(PHYSICAL_DATASET));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParentEntity_schemaFieldViaDataset() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_DATASET);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParentEntity(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD));
  }

  @Test
  public void
      testIsAuthorizedToEditLogicalParentEntity_schemaFieldViaEntityUrnOnlyAfterDatasetDenied() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_SCHEMA_FIELD);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParentEntity(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD));

    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(PHYSICAL_DATASET))),
        times(1));
    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(PHYSICAL_SCHEMA_FIELD))),
        times(1));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParent_datasetPairUsesPerSideChecks() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_DATASET, LOGICAL_DATASET);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParent(
            mockAuthSession, PHYSICAL_DATASET, LOGICAL_DATASET));

    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(PHYSICAL_DATASET))),
        times(1));
    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(LOGICAL_DATASET))),
        times(1));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParent_schemaFieldPairUsesDatasetPerSide() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_DATASET, LOGICAL_DATASET);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParent(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD, LOGICAL_SCHEMA_FIELD));

    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(PHYSICAL_DATASET))),
        times(1));
    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(LOGICAL_DATASET))),
        times(1));
    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(PHYSICAL_SCHEMA_FIELD))),
        times(0));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParentEntity_schemaFieldViaEntityUrn() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_SCHEMA_FIELD);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParentEntity(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParent_allowsBothSidesViaDatasets() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_DATASET, LOGICAL_DATASET);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParent(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD, LOGICAL_SCHEMA_FIELD));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParent_deniesPhysicalOnly() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_DATASET);

    Assert.assertFalse(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParent(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD, LOGICAL_SCHEMA_FIELD));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParent_deniesLogicalOnly() {
    mockEditEntityAuthorizedOnUrns(LOGICAL_DATASET);

    Assert.assertFalse(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParent(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD, LOGICAL_SCHEMA_FIELD));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParent_allowsMixedDatasetAndSchemaFieldGrants() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_DATASET, LOGICAL_SCHEMA_FIELD);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParent(
            mockAuthSession, PHYSICAL_DATASET, LOGICAL_SCHEMA_FIELD));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParent_allowsSchemaFieldPairViaPerSideFieldUrns() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_SCHEMA_FIELD, LOGICAL_SCHEMA_FIELD);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParent(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD, LOGICAL_SCHEMA_FIELD));

    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(PHYSICAL_DATASET))),
        times(1));
    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(LOGICAL_DATASET))),
        times(1));
    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(PHYSICAL_SCHEMA_FIELD))),
        times(1));
    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorizedEntityUrns(
                eq(mockAuthSession), eq(UPDATE), eq(Set.of(LOGICAL_SCHEMA_FIELD))),
        times(1));
  }

  @Test
  public void testIsAuthorizedToEditLogicalParent_clearParentViaDatasetOnly() {
    mockEditEntityAuthorizedOnUrns(PHYSICAL_DATASET);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParent(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD, null));
  }

  @Test
  public void testFilterUnauthorizedToEditLogicalParent_allowsWhenBothSidesPass() {
    Urn child = PHYSICAL_SCHEMA_FIELD;
    Urn parent = LOGICAL_SCHEMA_FIELD;
    mockEditEntityAuthorizedOnUrns(PHYSICAL_DATASET, LOGICAL_DATASET);

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToEditLogicalParent(
            mockAuthSession, Map.of(child, Set.of(child, parent)));

    Assert.assertTrue(unauthorized.isEmpty());
  }

  private void mockEditEntityAuthorizedOnUrns(Urn... authorizedUrns) {
    Set<Urn> authorized = Set.of(authorizedUrns);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorizedEntityUrns(
                    eq(mockAuthSession), eq(UPDATE), any(Collection.class)))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Collection<Urn> urns = invocation.getArgument(2);
              return authorized.containsAll(urns);
            });
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
  public void testFilterViewableQueryEntities_viewAllQueriesPrivilegeGrantsOrphanQuery() {
    // No subjects aspect at all — the case VIEW_ENTITY_QUERIES can never satisfy.
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of()));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession), eq(PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE)))
        .thenReturn(true);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, List.of(QUERY_URN));

    Assert.assertEquals(
        viewable,
        Set.of(QUERY_URN),
        "VIEW_ALL_QUERIES is the deliberate escape valve for orphan queries");
  }

  @Test
  public void
      testFilterViewableQueryEntities_viewAllQueriesPrivilegeGrantsOrphanQueryInStrictMode() {
    // The documented limitation ("orphans always denied in requireAllSubjects mode") is about
    // VIEW_ENTITY_QUERIES specifically; VIEW_ALL_QUERIES bypasses it in both modes.
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of()));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession), eq(PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE)))
        .thenReturn(true);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            List.of(QUERY_URN),
            /* requireAllSubjects= */ true);

    Assert.assertEquals(viewable, Set.of(QUERY_URN));
  }

  @Test
  public void testFilterViewableQueryEntities_withoutViewAllQueriesPrivilegeOrphanStillDenied() {
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of()));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession), eq(PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE)))
        .thenReturn(false);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, List.of(QUERY_URN));

    Assert.assertTrue(viewable.isEmpty());
  }

  @Test
  public void
      testFilterViewableQueryEntities_viewAllQueriesPrivilegeShortCircuitsPerSubjectDenial() {
    // Proves a true tier-1 short-circuit, not merely an orphan-case special case: even a query
    // WITH subjects, where the per-dataset check would deny, is granted via VIEW_ALL_QUERIES.
    stubTwoSubjectQuery();
    stubSubjectGrant(SUBJECT_DATASET, false);
    stubSubjectGrant(ASSET_URN, false);

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession), eq(PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE)))
        .thenReturn(true);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, List.of(QUERY_URN));

    Assert.assertEquals(viewable, Set.of(QUERY_URN));
  }

  @Test
  public void testFilterViewableQueryEntities_allowsViaViewEntityQueriesPrivilegeOnSubject() {
    QuerySubjects querySubjects = new QuerySubjects();
    QuerySubject subject = new QuerySubject();
    subject.setEntity(SUBJECT_DATASET);
    querySubjects.setSubjects(new QuerySubjectArray(subject));
    Aspect subjectsAspect = new Aspect(querySubjects.data());

    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of(QUERY_SUBJECTS_ASPECT_NAME, subjectsAspect)));

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(
                        new EntitySpec(
                            SUBJECT_DATASET.getEntityType(), SUBJECT_DATASET.toString()))))
        .thenReturn(true);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, List.of(QUERY_URN));

    Assert.assertEquals(viewable, Set.of(QUERY_URN));
  }

  @Test
  public void testFilterViewableQueryEntities_pageViewAloneNoLongerSufficient() {
    QuerySubjects querySubjects = new QuerySubjects();
    QuerySubject subject = new QuerySubject();
    subject.setEntity(SUBJECT_DATASET);
    querySubjects.setSubjects(new QuerySubjectArray(subject));
    Aspect subjectsAspect = new Aspect(querySubjects.data());

    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of(QUERY_SUBJECTS_ASPECT_NAME, subjectsAspect)));

    // Entity-page visibility no longer implies query visibility; the explicit privilege check
    // (mocked to deny) is what decides.
    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(SUBJECT_DATASET)))
        .thenReturn(true);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, List.of(QUERY_URN));

    Assert.assertTrue(viewable.isEmpty());
  }

  @Test
  public void testFilterViewableQueryEntities_anySubjectModeAllowsPartialGrant() {
    stubTwoSubjectQuery();
    stubSubjectGrant(SUBJECT_DATASET, true);
    stubSubjectGrant(ASSET_URN, false);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            List.of(QUERY_URN),
            /* requireAllSubjects= */ false);

    Assert.assertEquals(
        viewable, Set.of(QUERY_URN), "any-subject mode: a grant on one subject dataset suffices");
  }

  @Test
  public void testFilterViewableQueryEntities_strictModeDeniesPartialGrant() {
    stubTwoSubjectQuery();
    stubSubjectGrant(SUBJECT_DATASET, true);
    stubSubjectGrant(ASSET_URN, false);

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            List.of(QUERY_URN),
            /* requireAllSubjects= */ true);

    Assert.assertTrue(
        viewable.isEmpty(), "strict mode: every subject dataset must grant the privilege");
  }

  @Test
  public void testFilterViewableQueryEntities_anySubjectModeStillDeniesQueryWithNoSubjects() {
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of()));

    Set<Urn> viewable =
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            List.of(QUERY_URN),
            /* requireAllSubjects= */ false);

    Assert.assertTrue(viewable.isEmpty(), "no subjects means nothing to grant against, any mode");
  }

  /**
   * End-to-end regression for the default enforcement mode (queryEntities.enabled=true,
   * requireAllSubjects=COMPAT — i.e. no explicit QUERY_ENTITY_AUTHORIZATION_* overrides at all),
   * combining filterViewableQueryEntities's per-subject counting with requireAllQuerySubjects's
   * VIEW_AUTHORIZATION_ENABLED-driven resolution for a two-subject-dataset query: with
   * VIEW_AUTHORIZATION_ENABLED on, holding VIEW_ENTITY_QUERIES on one of two subjects is denied, on
   * both of two succeeds, and on neither is denied; with VIEW_AUTHORIZATION_ENABLED off, one of two
   * already suffices, matching the pre-existing default.
   */
  @Test
  public void testFilterViewableQueryEntities_defaultModeAcrossViewAuthorizationEnabled() {
    stubTwoSubjectQuery();

    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig defaultConfig =
        ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder().build();
    boolean requireAllViewAuthOn =
        EntityAspectAuthorizationUtils.requireAllQuerySubjects(
            queryAuthContext(defaultConfig, true));
    boolean requireAllViewAuthOff =
        EntityAspectAuthorizationUtils.requireAllQuerySubjects(
            queryAuthContext(defaultConfig, false));
    Assert.assertTrue(
        requireAllViewAuthOn,
        "default (COMPAT) mode must require all subjects once VIEW_AUTHORIZATION_ENABLED is on");
    Assert.assertFalse(
        requireAllViewAuthOff,
        "default (COMPAT) mode must be any-subject when VIEW_AUTHORIZATION_ENABLED is off");

    // VIEW_AUTHORIZATION_ENABLED on, neither of two subjects granted: denied.
    stubSubjectGrant(SUBJECT_DATASET, false);
    stubSubjectGrant(ASSET_URN, false);
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
                OperationFingerprint.EMPTY,
                mockAuthSession,
                mockAspectRetriever,
                List.of(QUERY_URN),
                requireAllViewAuthOn)
            .isEmpty(),
        "VIEW_AUTHORIZATION_ENABLED on, default mode: neither subject granted must deny");

    // VIEW_AUTHORIZATION_ENABLED on, one of two subjects granted: still denied.
    stubSubjectGrant(SUBJECT_DATASET, true);
    stubSubjectGrant(ASSET_URN, false);
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
                OperationFingerprint.EMPTY,
                mockAuthSession,
                mockAspectRetriever,
                List.of(QUERY_URN),
                requireAllViewAuthOn)
            .isEmpty(),
        "VIEW_AUTHORIZATION_ENABLED on, default mode: one of two subjects granted must deny");

    // VIEW_AUTHORIZATION_ENABLED on, both of two subjects granted: succeeds.
    stubSubjectGrant(SUBJECT_DATASET, true);
    stubSubjectGrant(ASSET_URN, true);
    Assert.assertEquals(
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            List.of(QUERY_URN),
            requireAllViewAuthOn),
        Set.of(QUERY_URN),
        "VIEW_AUTHORIZATION_ENABLED on, default mode: both subjects granted must succeed");

    // VIEW_AUTHORIZATION_ENABLED off, one of two subjects granted: already suffices.
    stubSubjectGrant(SUBJECT_DATASET, true);
    stubSubjectGrant(ASSET_URN, false);
    Assert.assertEquals(
        EntityAspectAuthorizationUtils.filterViewableQueryEntities(
            OperationFingerprint.EMPTY,
            mockAuthSession,
            mockAspectRetriever,
            List.of(QUERY_URN),
            requireAllViewAuthOff),
        Set.of(QUERY_URN),
        "VIEW_AUTHORIZATION_ENABLED off, default mode: one of two subjects granted must suffice");
  }

  private void stubTwoSubjectQuery() {
    QuerySubjects querySubjects = new QuerySubjects();
    QuerySubject subjectA = new QuerySubject();
    subjectA.setEntity(SUBJECT_DATASET);
    QuerySubject subjectB = new QuerySubject();
    subjectB.setEntity(ASSET_URN);
    querySubjects.setSubjects(new QuerySubjectArray(subjectA, subjectB));
    Aspect subjectsAspect = new Aspect(querySubjects.data());

    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(Map.of(QUERY_URN, Map.of(QUERY_SUBJECTS_ASPECT_NAME, subjectsAspect)));
  }

  @Test
  public void testQueryViewAuthorizationEnabled_configResolution() {
    // Missing config block means the default: enabled, COMPAT mode. requireAllQuerySubjects is
    // any-subject under COMPAT regardless of legacy view-auth state.
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isQueryViewAuthorizationEnabled(
            queryAuthContext(null, false)));
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.requireAllQuerySubjects(queryAuthContext(null, false)));

    // Explicitly disabled with legacy view-auth off: fully inactive (escape valve).
    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig disabled =
        ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
            .enabled(false)
            .build();
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.isQueryViewAuthorizationEnabled(
            queryAuthContext(disabled, false)));

    // Explicitly disabled but legacy view-auth on: active with the original strict semantics.
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isQueryViewAuthorizationEnabled(
            queryAuthContext(disabled, true)));
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.requireAllQuerySubjects(queryAuthContext(disabled, true)));

    // Enabled with operator-selected mode.
    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig strict =
        ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
            .enabled(true)
            .requireAllSubjects(ViewAuthorizationConfiguration.RequireAllSubjectsMode.TRUE)
            .build();
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.isQueryViewAuthorizationEnabled(
            queryAuthContext(strict, false)));
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.requireAllQuerySubjects(queryAuthContext(strict, false)));
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.requireAllQuerySubjectsForTopSqlQueries(
            queryAuthContext(strict, false)),
        "literal TRUE mode still locks topSqlQueries behind VIEW_ALL_QUERIES");

    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig anyMode =
        ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
            .enabled(true)
            .requireAllSubjects(ViewAuthorizationConfiguration.RequireAllSubjectsMode.FALSE)
            .build();
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.requireAllQuerySubjects(queryAuthContext(anyMode, false)));
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.requireAllQuerySubjectsForTopSqlQueries(
            queryAuthContext(anyMode, false)));
  }

  /**
   * COMPAT (the default) resolves {@link EntityAspectAuthorizationUtils#requireAllQuerySubjects}
   * uniformly by VIEW_AUTHORIZATION_ENABLED's runtime state — any-subject when it's off,
   * require-all when it's on — for every Query-entity-subject caller (direct Query reads,
   * listQueries, REST reads, and canViewEntity's search-masking branch all share this one method).
   * {@link EntityAspectAuthorizationUtils#requireAllQuerySubjectsForTopSqlQueries} is a deliberate
   * carve-out: always any-subject under COMPAT regardless of VIEW_AUTHORIZATION_ENABLED, since
   * topSqlQueries entries have no per-statement dataset association to verify against.
   */
  @Test
  public void testRequireAllQuerySubjects_compatModeTracksViewAuthorizationEnabled() {
    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig compat =
        ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
            .enabled(true)
            .requireAllSubjects(ViewAuthorizationConfiguration.RequireAllSubjectsMode.COMPAT)
            .build();

    io.datahubproject.metadata.context.OperationContext viewAuthOffContext =
        queryAuthContext(compat, false);
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.requireAllQuerySubjects(viewAuthOffContext),
        "COMPAT must be any-subject when VIEW_AUTHORIZATION_ENABLED is off");
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.requireAllQuerySubjectsForTopSqlQueries(viewAuthOffContext));

    io.datahubproject.metadata.context.OperationContext viewAuthOnContext =
        queryAuthContext(compat, true);
    Assert.assertTrue(
        EntityAspectAuthorizationUtils.requireAllQuerySubjects(viewAuthOnContext),
        "COMPAT must require all subjects once VIEW_AUTHORIZATION_ENABLED is on");
    Assert.assertFalse(
        EntityAspectAuthorizationUtils.requireAllQuerySubjectsForTopSqlQueries(viewAuthOnContext),
        "topSqlQueries stays any-subject under COMPAT even with VIEW_AUTHORIZATION_ENABLED on, so"
            + " ordinary per-dataset users keep seeing their dataset's own top queries without"
            + " VIEW_ALL_QUERIES");
  }

  private io.datahubproject.metadata.context.OperationContext queryAuthContext(
      ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig queryEntities,
      boolean legacyViewAuthEnabled) {
    io.datahubproject.metadata.context.OperationContext opContext =
        mock(io.datahubproject.metadata.context.OperationContext.class);
    io.datahubproject.metadata.context.OperationContextConfig config =
        mock(io.datahubproject.metadata.context.OperationContextConfig.class);
    when(opContext.getOperationContextConfig()).thenReturn(config);
    when(config.getViewAuthorizationConfiguration())
        .thenReturn(
            ViewAuthorizationConfiguration.builder()
                .enabled(legacyViewAuthEnabled)
                .queryEntities(queryEntities)
                .build());
    return opContext;
  }

  private void stubSubjectGrant(Urn datasetUrn, boolean granted) {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec(datasetUrn.getEntityType(), datasetUrn.toString()))))
        .thenReturn(granted);
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
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(
                        new EntitySpec(
                            SUBJECT_DATASET.getEntityType(), SUBJECT_DATASET.toString()))))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.canViewQueryEntity(
            OperationFingerprint.EMPTY, mockAuthSession, mockAspectRetriever, QUERY_URN));
  }

  @Test
  public void testCanViewSchemaFieldEntity_allowsViaParentDataset() {
    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(PHYSICAL_DATASET)))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD));
  }

  @Test
  public void testCanViewSchemaFieldEntity_allowsViaDirectSchemaFieldGrant() {
    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(PHYSICAL_DATASET)))
        .thenReturn(false);
    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(PHYSICAL_SCHEMA_FIELD)))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD));
  }

  @Test
  public void testCanViewSchemaFieldEntity_deniesWithoutParentOrDirectGrant() {
    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(PHYSICAL_DATASET)))
        .thenReturn(false);
    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(PHYSICAL_SCHEMA_FIELD)))
        .thenReturn(false);

    Assert.assertFalse(
        EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(
            mockAuthSession, PHYSICAL_SCHEMA_FIELD));
  }

  @Test
  public void testCanViewSchemaFieldEntity_nonSchemaFieldDelegatesToCanViewEntity() {
    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(PHYSICAL_DATASET)))
        .thenReturn(true);

    Assert.assertTrue(
        EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(mockAuthSession, PHYSICAL_DATASET));

    authUtilMockedStatic
        .when(() -> AuthUtil.canViewEntity(eq(mockAuthSession), eq(PHYSICAL_DATASET)))
        .thenReturn(false);

    Assert.assertFalse(
        EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(mockAuthSession, PHYSICAL_DATASET));
  }

  @Test
  public void testIsSchemaFieldEntity() {
    Assert.assertTrue(EntityAspectAuthorizationUtils.isSchemaFieldEntity(PHYSICAL_SCHEMA_FIELD));
    Assert.assertFalse(EntityAspectAuthorizationUtils.isSchemaFieldEntity(PHYSICAL_DATASET));
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
