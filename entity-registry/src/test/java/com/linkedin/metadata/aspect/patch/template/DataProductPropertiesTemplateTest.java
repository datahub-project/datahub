package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.metadata.aspect.patch.template.dataproduct.DataProductPropertiesTemplate;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataProductPropertiesTemplateTest {

  private static final DataProductPropertiesTemplate TEMPLATE = new DataProductPropertiesTemplate();
  private static final String DATASET_A =
      "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.orders,PROD)";
  private static final String DATASET_B =
      "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.order_events,PROD)";

  /** The patch DataProductPatchBuilder.add_asset(urn, output_port=True) emits. */
  private static JsonPatch addOutputPort(String assetUrn) {
    return Json.createPatch(
        Json.createArrayBuilder()
            .add(
                Json.createObjectBuilder()
                    .add("op", "add")
                    .add("path", "/assets/" + assetUrn)
                    .add(
                        "value",
                        Json.createObjectBuilder()
                            .add("destinationUrn", assetUrn)
                            .add("outputPort", true)))
            .build());
  }

  private static DataProductAssociation assetOf(DataProductProperties props, String assetUrn) {
    Assert.assertNotNull(props.getAssets());
    return props.getAssets().stream()
        .filter(asset -> assetUrn.equals(asset.getDestinationUrn().toString()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("asset not found: " + assetUrn));
  }

  @Test
  public void testAddAssetAsOutputPort() throws Exception {
    DataProductProperties initial = TEMPLATE.getDefault();

    DataProductProperties result = TEMPLATE.applyPatch(initial, addOutputPort(DATASET_A));

    Assert.assertTrue(assetOf(result, DATASET_A).isOutputPort());
  }

  @Test
  public void testOutputPortPatchPromotesExistingAssetAndLeavesOthersAlone() throws Exception {
    // A product curated in DataHub: one plain asset, one that a contract also covers.
    DataProductProperties initial = new DataProductProperties();
    initial.setAssets(
        new DataProductAssociationArray(
            new DataProductAssociation().setDestinationUrn(UrnUtils.getUrn(DATASET_A)),
            new DataProductAssociation().setDestinationUrn(UrnUtils.getUrn(DATASET_B))));

    DataProductProperties result = TEMPLATE.applyPatch(initial, addOutputPort(DATASET_A));

    Assert.assertEquals(result.getAssets().size(), 2);
    Assert.assertTrue(assetOf(result, DATASET_A).isOutputPort(), "patched asset is an output port");
    Assert.assertFalse(assetOf(result, DATASET_B).isOutputPort(), "other asset is untouched");
  }
}
