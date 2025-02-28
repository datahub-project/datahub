package com.linkedin.metadata.structuredproperties.hooks;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class StructuredPropertiesSoftDeleteTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  @Test
  public void testSoftDeleteFilter() throws URISyntaxException, CloneNotSupportedException {
    Urn propertyUrnA =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");
    StructuredPropertyDefinition stringPropertyDefA =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.string"));
    StructuredPropertyValueAssignment assignmentA =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrnA)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(0.0)));

    Urn propertyUrnB =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTimeDeleted");
    StructuredPropertyDefinition stringPropertyDefB =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.string"));
    StructuredPropertyValueAssignment assignmentB =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrnB)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(0.0)));

    StructuredPropertiesSoftDelete testHook =
        new StructuredPropertiesSoftDelete()
            .setConfig(
                AspectPluginConfig.builder()
                    .enabled(true)
                    .className(StructuredPropertiesSoftDelete.class.getName())
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName(DATASET_ENTITY_NAME)
                                .aspectName(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME)
                                .build()))
                    .build());

    StructuredProperties expectedAllValues = new StructuredProperties();
    expectedAllValues.setProperties(
        new StructuredPropertyValueAssignmentArray(assignmentA, assignmentB));

    RetrieverContext mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever())
        .thenReturn(
            new MockAspectRetriever(
                Map.of(
                    propertyUrnA,
                    List.of(stringPropertyDefA),
                    propertyUrnB,
                    List.of(stringPropertyDefB))));
    StructuredProperties test = expectedAllValues.copy();
    testHook.readMutation(
        TestMCP.ofOneBatchItemDatasetUrn(test, TEST_REGISTRY), mockRetrieverContext);
    assertEquals(
        test.getProperties().size(),
        2,
        "Expected all values because all definitions are NOT soft deleted");

    StructuredProperties expectedOneValue = new StructuredProperties();
    expectedOneValue.setProperties(new StructuredPropertyValueAssignmentArray(assignmentA));
    test = expectedAllValues.copy();

    mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever())
        .thenReturn(
            new MockAspectRetriever(
                Map.of(
                    propertyUrnA,
                    List.of(stringPropertyDefA),
                    propertyUrnB,
                    List.of(stringPropertyDefB, new Status().setRemoved(true)))));
    testHook.readMutation(
        TestMCP.ofOneBatchItemDatasetUrn(test, TEST_REGISTRY), mockRetrieverContext);
    assertEquals(
        test.getProperties().size(), 1, "Expected 1 value because 1 definition is soft deleted");
  }
}
