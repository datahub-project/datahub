package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
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
import org.testng.annotations.DataProvider;
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
                    eq(mockAuthSession), any(Map.class), any(Map.class), any(Map.class)))
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
                    eq(mockAuthSession), any(Map.class), any(Map.class), any(Map.class)))
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
                    eq(mockAuthSession), any(Set.class), any(Map.class), any(Map.class)))
        .thenReturn(Set.of());

    TestMCP item = upsertItem(proposed);
    validate(item);

    authUtilsMockedStatic.verify(
        () ->
            EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
                eq(mockAuthSession), eq(Set.of(DATA_PRODUCT_URN)), any(Map.class), any(Map.class)));
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
                    eq(mockAuthSession), any(Set.class), any(Map.class), any(Map.class)))
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
                    eq(mockAuthSession), any(Map.class), any(Map.class), any(Map.class)))
        .thenReturn(Set.of());

    TestMCP item = upsertItem(proposed);
    validate(item);

    authUtilsMockedStatic.verify(
        () ->
            EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                eq(mockAuthSession), any(Map.class), any(Map.class), any(Map.class)));
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
                    eq(mockAuthSession), any(Map.class), any(Map.class), any(Map.class)))
        .thenReturn(Set.of());

    TestMCP item = upsertItem(proposed);
    validate(item);

    authUtilsMockedStatic.verify(
        () ->
            EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                eq(mockAuthSession), any(Map.class), any(Map.class), any(Map.class)));
  }

  // A PATCH arrives as a PatchItemImpl by default, or as a ProposedItem under alternate MCP
  // validation. Both must be resolved against the stored aspect before authorizing.
  @DataProvider(name = "patchItemBuilders")
  public Object[][] patchItemBuilders() {
    return new Object[][] {{true}, {false}};
  }

  @Test(dataProvider = "patchItemBuilders")
  public void testPatchAddingAssetChecksOnlyTheAddedAsset(boolean alternateValidation) {
    mockCurrentProperties(propertiesWithAssets(MEMBER_DATASET_URN));
    authUtilsMockedStatic
        .when(
            () ->
                EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                    eq(mockAuthSession), any(Map.class), any(Map.class), any(Map.class)))
        .thenReturn(Set.of());

    validate(patchItem(alternateValidation, addAssetOp(OTHER_DATASET_URN))).toList();

    authUtilsMockedStatic.verify(
        () ->
            EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                eq(mockAuthSession),
                eq(Map.of(DATA_PRODUCT_URN, Set.of(OTHER_DATASET_URN))),
                any(Map.class),
                any(Map.class)));
  }

  @Test(dataProvider = "patchItemBuilders")
  public void testPatchAddingFirstAssetIsDeniedWithoutPrivilege(boolean alternateValidation) {
    mockCurrentProperties(new DataProductProperties());
    authUtilsMockedStatic
        .when(
            () ->
                EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
                    eq(mockAuthSession), any(Map.class), any(Map.class), any(Map.class)))
        .thenReturn(Set.of(DATA_PRODUCT_URN));

    Assert.assertTrue(
        validate(patchItem(alternateValidation, addAssetOp(OTHER_DATASET_URN)))
            .findAny()
            .isPresent());
  }

  @Test(dataProvider = "patchItemBuilders")
  public void testPatchRenameIsDeniedWithoutPrivilege(boolean alternateValidation) {
    DataProductProperties current = new DataProductProperties();
    current.setName("Original name");
    mockCurrentProperties(current);
    authUtilsMockedStatic
        .when(
            () ->
                EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
                    eq(mockAuthSession), any(Set.class), any(Map.class), any(Map.class)))
        .thenReturn(Set.of(DATA_PRODUCT_URN));

    Assert.assertTrue(
        validate(
                patchItem(
                    alternateValidation,
                    "{\"op\":\"add\",\"path\":\"/name\",\"value\":\"Updated name\"}"))
            .findAny()
            .isPresent());
  }

  private void mockCurrentProperties(DataProductProperties current) {
    Aspect aspect = new Aspect(current.data());
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, Map.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME, aspect)));
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATA_PRODUCT_URN)), eq(Set.of(DOMAINS_ASPECT_NAME))))
        .thenReturn(Map.of(DATA_PRODUCT_URN, Map.of()));
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

  private static String addAssetOp(Urn assetUrn) {
    return String.format(
        "{\"op\":\"add\",\"path\":\"/assets/%s\",\"value\":{\"destinationUrn\":\"%s\"}}",
        assetUrn, assetUrn);
  }

  // Same JSON Patch array shape the Python DataProductPatchBuilder emits.
  private static BatchItem patchItem(boolean alternateValidation, String op) {
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(DATA_PRODUCT_URN)
            .setEntityType(DATA_PRODUCT_URN.getEntityType())
            .setAspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
            .setChangeType(ChangeType.PATCH)
            .setAspect(
                new GenericAspect()
                    .setContentType("application/json")
                    .setValue(ByteString.copyString("[" + op + "]", StandardCharsets.UTF_8)));
    TestEntityRegistry registry = new TestEntityRegistry();
    return alternateValidation
        ? ProposedItem.builder().build(mcp, AuditStampUtils.createDefaultAuditStamp(), registry)
        : PatchItemImpl.builder().build(mcp, AuditStampUtils.createDefaultAuditStamp(), registry);
  }

  private Stream<AspectValidationException> validate(BatchItem item) {
    return validator.validateProposedAspectsWithAuth(
        OperationFingerprint.EMPTY,
        Collections.singletonList(item),
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever)
            .getRetrieverContext(),
        mockAuthSession);
  }
}
