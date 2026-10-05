package com.linkedin.datahub.graphql.types.api.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.api.ApiProperties;
import com.linkedin.api.ApiSignature;
import com.linkedin.api.HttpMethod;
import com.linkedin.api.RestApiProperties;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.Api;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import org.testng.annotations.Test;

public class ApiMapperTest {

  private static SchemaField schemaField(String path, boolean nullable, String description) {
    SchemaField field = new SchemaField();
    field.setFieldPath(path);
    field.setType(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    field.setNativeDataType("string");
    field.setNullable(nullable);
    field.setDescription(description);
    return field;
  }

  private static EntityResponse response(Urn urn, EnvelopedAspectMap aspects) {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(urn);
    entityResponse.setEntityName(Constants.API_ENTITY_NAME);
    entityResponse.setAspects(aspects);
    return entityResponse;
  }

  @Test
  public void testMapApiWithPropertiesAndSignature() {
    Urn apiUrn = UrnUtils.getUrn("urn:li:api:langchain.search");

    // Identity lives on apiProperties.
    ApiProperties properties = new ApiProperties();
    properties.setName("search");
    properties.setDescription("Search things");
    properties.setExternalUrl("https://docs.example.com/search");

    // The input/output signature lives on its own apiSignature aspect.
    ApiSignature signature = new ApiSignature();
    signature.setSchemaDefinition("{\"type\":\"object\"}");
    signature.setInputFields(new SchemaFieldArray(schemaField("q", false, "the query")));
    signature.setOutputFields(new SchemaFieldArray(schemaField("result", true, "the result")));

    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("MCP_TOOL"));

    Status status = new Status();
    status.setRemoved(false);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.API_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(properties.data())));
    aspects.put(
        Constants.API_SIGNATURE_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(signature.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));
    aspects.put(
        Constants.STATUS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(status.data())));

    Api result = ApiMapper.map(null, response(apiUrn, aspects));

    assertNotNull(result);
    assertEquals(result.getUrn(), apiUrn.toString());
    assertEquals(result.getType(), EntityType.API);

    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().getName(), "search");
    assertEquals(result.getProperties().getDescription(), "Search things");
    assertEquals(result.getProperties().getExternalUrl(), "https://docs.example.com/search");

    assertNotNull(result.getSignature());
    assertEquals(result.getSignature().getSchemaDefinition(), "{\"type\":\"object\"}");
    assertNotNull(result.getSignature().getInputFields());
    assertEquals(result.getSignature().getInputFields().size(), 1);
    assertEquals(result.getSignature().getInputFields().get(0).getFieldPath(), "q");
    assertNotNull(result.getSignature().getOutputFields());
    assertEquals(result.getSignature().getOutputFields().get(0).getFieldPath(), "result");

    assertNotNull(result.getSubTypes());
    assertEquals(result.getSubTypes().getTypeNames().get(0), "MCP_TOOL");
    assertNotNull(result.getStatus());
    assertFalse(result.getStatus().getRemoved());
    assertNull(result.getRestProperties());
  }

  @Test
  public void testMapRestEndpointProperties() {
    Urn apiUrn = UrnUtils.getUrn("urn:li:api:order-entry-api/GET/orders/{orderId}");

    RestApiProperties restProperties = new RestApiProperties();
    restProperties.setMethod(HttpMethod.GET);
    restProperties.setPath("/orders/{orderId}");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.REST_API_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(restProperties.data())));

    Api result = ApiMapper.map(null, response(apiUrn, aspects));

    assertNotNull(result.getRestProperties());
    assertEquals(
        result.getRestProperties().getMethod(),
        com.linkedin.datahub.graphql.generated.HttpMethod.GET);
    assertEquals(result.getRestProperties().getPath(), "/orders/{orderId}");
  }

  @Test
  public void testMapApiWithoutSignatureAspect() {
    // An API with no signature yet: name falls back to the urn id and the
    // signature is simply absent.
    Urn apiUrn = UrnUtils.getUrn("urn:li:api:langchain.noop");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.API_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(new ApiProperties().data())));

    Api result = ApiMapper.map(null, response(apiUrn, aspects));

    assertNotNull(result);
    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().getName(), apiUrn.getId());
    assertNull(result.getSignature());
  }
}
