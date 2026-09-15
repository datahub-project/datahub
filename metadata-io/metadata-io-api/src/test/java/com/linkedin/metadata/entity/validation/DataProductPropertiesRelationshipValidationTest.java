package com.linkedin.metadata.entity.validation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Exercises {@link ValidationApiUtils#validateRecordTemplate} against the real entity registry for
 * the {@code DataProductProperties.assets[*].destinationUrn} relationship whitelist.
 *
 * <p>Motivation: the UI "Set Data Product" flow on a Document profile failed at ingestion with
 * {@code "Entity type for urn: urn:li:document:... is not a valid destination for field path:
 * /assets/*\/destinationUrn"}. The failure is driven by the {@code @Relationship.entityTypes} array
 * on {@code DataProductProperties.assets} being read by {@link
 * com.linkedin.metadata.utils.EntityRegistryUrnValidator}. These tests pin that contract.
 */
public class DataProductPropertiesRelationshipValidationTest {

  private static final Urn DATA_PRODUCT_URN =
      UrnUtils.getUrn("urn:li:dataProduct:test-data-product");

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final EntityRegistry entityRegistry = opContext.getEntityRegistry();
  private AspectRetriever aspectRetriever;

  @BeforeClass
  public void setUp() {
    aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  @DataProvider(name = "validDestinationUrns")
  public Object[][] validDestinationUrns() {
    return new Object[][] {
      // Pre-existing whitelist members — baseline guards against accidental narrowing.
      {"dataset", UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,example.table,PROD)")},
      {"mlModel", UrnUtils.getUrn("urn:li:mlModel:(urn:li:dataPlatform:sagemaker,example,PROD)")},
      {"notebook", UrnUtils.getUrn("urn:li:notebook:(hex,abc123)")},
      // The case that fails today — added to the whitelist in this change.
      {"document", UrnUtils.getUrn("urn:li:document:2fa9f2fb-b911-4300-ad2b-1859fc178f01")}
    };
  }

  @Test(dataProvider = "validDestinationUrns")
  public void testDataProductContainsAcceptsWhitelistedDestinationTypes(
      String label, Urn destinationUrn) {
    final DataProductProperties aspect = buildAspectWithAsset(destinationUrn);
    final EntitySpec dataProductSpec = entityRegistry.getEntitySpec("dataProduct");

    try {
      ValidationApiUtils.validateRecordTemplate(
          dataProductSpec, DATA_PRODUCT_URN, aspect, aspectRetriever);
    } catch (ValidationException e) {
      fail(
          "Expected "
              + label
              + " URN to be a valid DataProductContains destination, but validation failed: "
              + e.getMessage(),
          e);
    }
  }

  @Test
  public void testDataProductContainsRejectsNonWhitelistedDestinationType() {
    // corpuser is not part of the DataProductContains relationship whitelist and should remain
    // rejected — guards against accidentally widening the whitelist beyond assets.
    final Urn invalidDestination = UrnUtils.getUrn("urn:li:corpuser:alice@example.com");
    final DataProductProperties aspect = buildAspectWithAsset(invalidDestination);
    final EntitySpec dataProductSpec = entityRegistry.getEntitySpec("dataProduct");

    try {
      ValidationApiUtils.validateRecordTemplate(
          dataProductSpec, DATA_PRODUCT_URN, aspect, aspectRetriever);
      fail("Expected ValidationException for corpuser URN in DataProductProperties.assets");
    } catch (ValidationException e) {
      // Assert the failure references the aspect + the offending field path, not exact wording —
      // wording is incidental; path/aspect identity is the regression class we care about.
      final String message = e.getMessage();
      assertTrue(
          message.contains("dataProduct"),
          "Expected error message to name the aspect under validation. Got: " + message);
      assertTrue(
          message.contains("/assets/0/destinationUrn"),
          "Expected error message to reference the offending field path. Got: " + message);
    }
  }

  private static DataProductProperties buildAspectWithAsset(Urn destinationUrn) {
    final DataProductAssociation association = new DataProductAssociation();
    association.setDestinationUrn(destinationUrn);

    final DataProductAssociationArray assets = new DataProductAssociationArray();
    assets.add(association);

    final DataProductProperties properties = new DataProductProperties();
    properties.setName("Test Data Product");
    properties.setAssets(assets);
    return properties;
  }
}
