package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProductMembershipAuthorizationValidatorTest {

  private static final Urn DATA_PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:auth-test");
  private static final Urn MEMBER_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,member,PROD)");
  private static final Urn OTHER_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,other,PROD)");

  private DataProductMembershipAuthorizationValidator validator;
  private AuthorizationSession mockAuthSession;
  private AspectRetriever mockAspectRetriever;
  private MockedStatic<EntityAspectAuthorizationUtils> authUtilsMockedStatic;

  @BeforeMethod
  public void setup() {
    authUtilsMockedStatic = Mockito.mockStatic(EntityAspectAuthorizationUtils.class);
    validator = new DataProductMembershipAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(DataProductMembershipAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("dataProduct")
                        .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
                        .build()))
            .build());
    mockAuthSession = Mockito.mock(AuthorizationSession.class);
    mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(new TestEntityRegistry());
  }

  @AfterMethod
  public void tearDown() {
    authUtilsMockedStatic.close();
  }

  @Test
  public void testDenyWithoutManageDataProductsOnDomain() {
    DataProductProperties proposed = propertiesWithAssets(MEMBER_DATASET_URN);
    mockCurrentProperties(new DataProductProperties());

    authUtilsMockedStatic
        .when(
            () ->
                EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                    any(),
                    eq(mockAuthSession),
                    eq(mockAspectRetriever),
                    any(Map.class),
                    any(Map.class)))
        .thenReturn(Set.of(DATA_PRODUCT_URN));

    TestMCP item = upsertItem(proposed);
    Stream<AspectValidationException> result = validate(item);

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testAllowWithManageDataProductsOnDomain() {
    DataProductProperties proposed = propertiesWithAssets(MEMBER_DATASET_URN);
    mockCurrentProperties(new DataProductProperties());

    authUtilsMockedStatic
        .when(
            () ->
                EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                    any(),
                    eq(mockAuthSession),
                    eq(mockAspectRetriever),
                    any(Map.class),
                    any(Map.class)))
        .thenReturn(Set.of());

    TestMCP item = upsertItem(proposed);
    Stream<AspectValidationException> result = validate(item);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testNameChangeTriggersAuthorizationCheck() {
    DataProductProperties current = propertiesWithAssets(MEMBER_DATASET_URN);
    current.setName("Original name");
    DataProductProperties proposed = propertiesWithAssets(MEMBER_DATASET_URN);
    proposed.setName("Updated name only");
    mockCurrentProperties(current);

    authUtilsMockedStatic
        .when(
            () ->
                EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
                    any(), any(), any(), any(Set.class), any(Map.class)))
        .thenReturn(Set.of());

    TestMCP item = upsertItem(proposed);
    validate(item);

    authUtilsMockedStatic.verify(
        () ->
            EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
                any(),
                eq(mockAuthSession),
                eq(mockAspectRetriever),
                eq(Set.of(DATA_PRODUCT_URN)),
                any(Map.class)));
  }

  @Test
  public void testDenyNameChangeWithoutPrivilege() {
    DataProductProperties current = new DataProductProperties();
    current.setName("Original name");
    DataProductProperties proposed = new DataProductProperties();
    proposed.setName("Updated name");
    mockCurrentProperties(current);

    authUtilsMockedStatic
        .when(
            () ->
                EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
                    any(), any(), any(), any(Set.class), any(Map.class)))
        .thenReturn(Set.of(DATA_PRODUCT_URN));

    TestMCP item = upsertItem(proposed);
    Stream<AspectValidationException> result = validate(item);

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testAssetsDeltaTriggersAuthorizationCheck() {
    DataProductProperties current = propertiesWithAssets(MEMBER_DATASET_URN);
    DataProductProperties proposed = propertiesWithAssets(OTHER_DATASET_URN);
    mockCurrentProperties(current);

    authUtilsMockedStatic
        .when(
            () ->
                EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                    any(), any(), any(), any(Map.class), any(Map.class)))
        .thenReturn(Set.of());

    TestMCP item = upsertItem(proposed);
    validate(item);

    authUtilsMockedStatic.verify(
        () ->
            EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                any(),
                eq(mockAuthSession),
                eq(mockAspectRetriever),
                any(Map.class),
                any(Map.class)));
  }

  @Test
  public void testAssetRemovalTriggersAuthorizationCheck() {
    DataProductProperties current = propertiesWithAssets(MEMBER_DATASET_URN);
    DataProductProperties proposed = new DataProductProperties();
    mockCurrentProperties(current);

    authUtilsMockedStatic
        .when(
            () ->
                EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                    any(),
                    eq(mockAuthSession),
                    eq(mockAspectRetriever),
                    any(Map.class),
                    any(Map.class)))
        .thenReturn(Set.of());

    TestMCP item = upsertItem(proposed);
    validate(item);

    authUtilsMockedStatic.verify(
        () ->
            EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                any(),
                eq(mockAuthSession),
                eq(mockAspectRetriever),
                any(Map.class),
                any(Map.class)));
  }

  private void mockCurrentProperties(DataProductProperties current) {
    Aspect aspect = new Aspect(current.data());
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, Map.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME, aspect)));
  }

  private static DataProductProperties propertiesWithAssets(Urn assetUrn) {
    DataProductProperties properties = new DataProductProperties();
    DataProductAssociation association = new DataProductAssociation();
    association.setDestinationUrn(assetUrn);
    DataProductAssociationArray assets = new DataProductAssociationArray();
    assets.add(association);
    properties.setAssets(assets);
    return properties;
  }

  private TestMCP upsertItem(DataProductProperties properties) {
    return TestMCP.ofOneUpsertItem(DATA_PRODUCT_URN, properties, new TestEntityRegistry()).stream()
        .map(i -> (TestMCP) i)
        .findFirst()
        .get();
  }

  private Stream<AspectValidationException> validate(TestMCP item) {
    return validator.validateProposedAspectsWithAuth(
        OperationFingerprint.EMPTY,
        Collections.singletonList(item),
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever)
            .getRetrieverContext(),
        mockAuthSession);
  }
}
