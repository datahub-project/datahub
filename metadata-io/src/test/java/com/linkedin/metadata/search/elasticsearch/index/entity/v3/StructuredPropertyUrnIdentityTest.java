package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class StructuredPropertyUrnIdentityTest {

  @Test
  public void testProjectsParsedIdentityBesideTheExactUrn() throws Exception {
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:steward");
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setQualifiedName("steward")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.urn"))
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    OperationContext opContext = operationContext(propertyUrn, definition);
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document
        .putArray("structuredProperties.steward")
        .add("urn:li:tag:Confidential")
        .add("urn:li:dataset:(urn:li:dataPlatform:hive,sales.orders,PROD)");

    StructuredProperties values =
        new StructuredProperties()
            .setProperties(
                new StructuredPropertyValueAssignmentArray(
                    new StructuredPropertyValueAssignment()
                        .setPropertyUrn(propertyUrn)
                        .setValues(
                            new PrimitivePropertyValueArray(
                                PrimitivePropertyValue.create("urn:li:tag:Confidential"),
                                PrimitivePropertyValue.create(
                                    "urn:li:dataset:(urn:li:dataPlatform:hive,sales.orders,PROD)")))));
    StructuredPropertyUrnIdentity.project(opContext, document, values);

    assertEquals(
        document.get("structuredProperties.steward").get(0).asText(), "urn:li:tag:Confidential");
    assertEquals(
        document.get("structuredProperties.steward__urn_name").get(0).asText(), "Confidential");
    assertEquals(
        document.get("structuredProperties.steward__urn_name").get(1).asText(), "sales.orders");
    assertFalse(document.get("structuredProperties.steward__urn_name").toString().contains("hive"));
  }

  @Test
  public void testParsedNameFollowsQueryByDefaultAndCopyTo() {
    EntitySpec dataset =
        TestOperationContexts.systemContextNoSearchAuthorization()
            .getEntityRegistry()
            .getEntitySpec("dataset");
    StructuredPropertyDefinition queried =
        urnDefinition().setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    StructuredPropertyDefinition filtered =
        urnDefinition().setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(false));
    StructuredPropertyDefinition copied =
        urnDefinition()
            .setSearchConfiguration(
                new DataHubSearchConfig()
                    .setQueryByDefault(false)
                    .setCopyTo(new StringArray("name")));

    assertTrue(StructuredPropertyUrnIdentity.indexesParsedName(queried, List.of(dataset)));
    assertFalse(StructuredPropertyUrnIdentity.indexesParsedName(filtered, List.of(dataset)));
    assertTrue(StructuredPropertyUrnIdentity.indexesParsedName(copied, List.of(dataset)));
  }

  /** No search configuration: the property is filtered on the exact URN and is not full text. */
  @Test
  public void testFilterOnlyUrnDoesNotProjectAParsedName() throws Exception {
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:steward");
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setQualifiedName("steward")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.urn"));
    OperationContext opContext = operationContext(propertyUrn, definition);
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.putArray("structuredProperties.steward").add("urn:li:tag:Confidential");

    StructuredPropertyUrnIdentity.project(opContext, document, properties(propertyUrn));

    assertTrue(document.path("structuredProperties.steward__urn_name").isMissingNode());
  }

  @Test
  public void testClearsIdentityWhenTheValueIsRemoved() throws Exception {
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:steward");
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setQualifiedName("steward")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.urn"))
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.putNull("structuredProperties.steward");

    StructuredPropertyUrnIdentity.project(
        operationContext(propertyUrn, definition), document, properties(propertyUrn));

    assertTrue(document.get("structuredProperties.steward__urn_name").isNull());
  }

  private static StructuredPropertyDefinition urnDefinition() {
    return new StructuredPropertyDefinition()
        .setQualifiedName("steward")
        .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
        .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.urn"));
  }

  private static StructuredProperties properties(Urn propertyUrn) {
    return new StructuredProperties()
        .setProperties(
            new StructuredPropertyValueAssignmentArray(
                new StructuredPropertyValueAssignment()
                    .setPropertyUrn(propertyUrn)
                    .setValues(
                        new PrimitivePropertyValueArray(
                            PrimitivePropertyValue.create("urn:li:tag:Confidential")))));
  }

  private static OperationContext operationContext(
      Urn propertyUrn, StructuredPropertyDefinition definition) throws Exception {
    EntityRegistry registry =
        TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObjects(
            any(), eq(Set.of(propertyUrn)), eq(Set.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                propertyUrn,
                Map.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME, new Aspect(definition.data()))));
    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getEntityRegistry()).thenReturn(registry);
    when(opContext.getAspectRetriever()).thenReturn(retriever);
    return opContext;
  }
}
