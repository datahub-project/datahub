package com.linkedin.metadata.structuredproperties.hooks;

import static com.linkedin.events.metadata.ChangeType.UPSERT;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class StructuredPropertiesAssignmentMutatorTest {

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

    StructuredPropertiesAssignmentMutator testHook =
        new StructuredPropertiesAssignmentMutator()
            .setConfig(
                AspectPluginConfig.builder()
                    .enabled(true)
                    .className(StructuredPropertiesAssignmentMutator.class.getName())
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
        OperationFingerprint.EMPTY,
        TestMCP.ofOneBatchItemDatasetUrn(test, TEST_REGISTRY),
        mockRetrieverContext);
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
        OperationFingerprint.EMPTY,
        TestMCP.ofOneBatchItemDatasetUrn(test, TEST_REGISTRY),
        mockRetrieverContext);
    assertEquals(
        test.getProperties().size(), 1, "Expected 1 value because 1 definition is soft deleted");
  }

  @Test
  public void testWriteMutationDropsMissingDefinitionsWithWarning() throws URISyntaxException {
    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Urn propertyUrnA =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");
    Urn propertyUrnMissing =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.deleted");
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.string"));

    StructuredProperties properties =
        new StructuredProperties()
            .setProperties(
                new StructuredPropertyValueAssignmentArray(
                    new StructuredPropertyValueAssignment()
                        .setPropertyUrn(propertyUrnA)
                        .setValues(
                            new PrimitivePropertyValueArray(PrimitivePropertyValue.create(1.0))),
                    new StructuredPropertyValueAssignment()
                        .setPropertyUrn(propertyUrnMissing)
                        .setValues(
                            new PrimitivePropertyValueArray(PrimitivePropertyValue.create(2.0)))));

    StructuredPropertiesAssignmentMutator testHook =
        new StructuredPropertiesAssignmentMutator()
            .setDropMissingPropertyValuesWithWarning(true)
            .setConfig(
                AspectPluginConfig.builder()
                    .enabled(true)
                    .className(StructuredPropertiesAssignmentMutator.class.getName())
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName(DATASET_ENTITY_NAME)
                                .aspectName(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME)
                                .build()))
                    .build());

    RetrieverContext mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever())
        .thenReturn(new MockAspectRetriever(Map.of(propertyUrnA, List.of(definition))));

    ChangeMCP changeMcp =
        TestMCP.builder()
            .changeType(UPSERT)
            .urn(datasetUrn)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME))
            .recordTemplate(properties)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build();

    Pair<ChangeMCP, Boolean> result =
        testHook
            .writeMutation(OperationFingerprint.EMPTY, List.of(changeMcp), mockRetrieverContext)
            .findFirst()
            .orElseThrow();

    assertEquals(result.getSecond(), Boolean.TRUE);
    assertEquals(result.getFirst().getAspect(StructuredProperties.class).getProperties().size(), 1);
    assertEquals(
        result
            .getFirst()
            .getAspect(StructuredProperties.class)
            .getProperties()
            .get(0)
            .getPropertyUrn(),
        propertyUrnA);
  }

  @Test
  public void testWriteMutationRejectsWhenNoValidAssignmentsRemain() throws URISyntaxException {
    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Urn propertyUrnMissing =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.deleted");

    StructuredProperties properties =
        new StructuredProperties()
            .setProperties(
                new StructuredPropertyValueAssignmentArray(
                    new StructuredPropertyValueAssignment()
                        .setPropertyUrn(propertyUrnMissing)
                        .setValues(
                            new PrimitivePropertyValueArray(PrimitivePropertyValue.create(1.0)))));

    StructuredPropertiesAssignmentMutator testHook =
        new StructuredPropertiesAssignmentMutator()
            .setDropMissingPropertyValuesWithWarning(true)
            .setConfig(
                AspectPluginConfig.builder()
                    .enabled(true)
                    .className(StructuredPropertiesAssignmentMutator.class.getName())
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName(DATASET_ENTITY_NAME)
                                .aspectName(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME)
                                .build()))
                    .build());

    RetrieverContext mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(new MockAspectRetriever(Map.of()));

    ChangeMCP changeMcp =
        TestMCP.builder()
            .changeType(UPSERT)
            .urn(datasetUrn)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME))
            .recordTemplate(properties)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            testHook
                .writeMutation(OperationFingerprint.EMPTY, List.of(changeMcp), mockRetrieverContext)
                .toList());
  }
}
