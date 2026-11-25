package com.linkedin.metadata.aspect.patch.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.api.ApiSignature;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.api.ApiSignatureTemplate;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatchBuilder;
import org.testng.annotations.Test;

public class ApiSignatureTemplateTest {

  private static final String REQUEST_URN =
      "urn:li:dataset:(urn:li:dataPlatform:kafka,orders.GetOrderRequest,PROD)";
  private static final String RESPONSE_URN =
      "urn:li:dataset:(urn:li:dataPlatform:kafka,orders.Order,PROD)";

  private static JsonObjectBuilder edge(String destinationUrn) {
    return Json.createObjectBuilder().add("destinationUrn", destinationUrn);
  }

  @Test
  public void testAddDatasetEdgesKeepsInlineFields() throws Exception {
    ApiSignatureTemplate template = new ApiSignatureTemplate();

    // An ingested signature with an inline input field and no dataset references.
    ApiSignature existing = new ApiSignature();
    SchemaField field = new SchemaField();
    field.setFieldPath("order_id");
    field.setNativeDataType("string");
    field.setType(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    existing.setInputFields(new SchemaFieldArray(field));

    JsonPatchBuilder patch = Json.createPatchBuilder();
    patch.add("/inputDatasetEdges/" + REQUEST_URN, edge(REQUEST_URN).build());
    patch.add("/outputDatasetEdges/" + RESPONSE_URN, edge(RESPONSE_URN).build());

    ApiSignature result = template.applyPatch(existing, patch.build());

    // The inline field survives a patch that only touches the edges.
    assertEquals(result.getInputFields().get(0).getFieldPath(), "order_id");
    assertEquals(result.getInputDatasetEdges().get(0).getDestinationUrn().toString(), REQUEST_URN);
    assertEquals(
        result.getOutputDatasetEdges().get(0).getDestinationUrn().toString(), RESPONSE_URN);
  }

  @Test
  public void testRemoveOneDatasetEdge() throws Exception {
    ApiSignatureTemplate template = new ApiSignatureTemplate();

    ApiSignature existing = new ApiSignature();
    existing.setInputDatasetEdges(
        new EdgeArray(
            new Edge().setDestinationUrn(UrnUtils.getUrn(REQUEST_URN)),
            new Edge().setDestinationUrn(UrnUtils.getUrn(RESPONSE_URN))));

    JsonPatchBuilder patch = Json.createPatchBuilder();
    patch.remove("/inputDatasetEdges/" + RESPONSE_URN);

    ApiSignature result = template.applyPatch(existing, patch.build());

    assertEquals(result.getInputDatasetEdges().size(), 1);
    assertEquals(result.getInputDatasetEdges().get(0).getDestinationUrn().toString(), REQUEST_URN);
    assertTrue(result.getOutputDatasetEdges().isEmpty());
  }
}
