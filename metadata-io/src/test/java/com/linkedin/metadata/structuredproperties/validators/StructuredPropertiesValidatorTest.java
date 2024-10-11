package com.linkedin.metadata.structuredproperties.validators;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.structuredproperties.validation.StructuredPropertiesValidator;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StructuredPropertiesValidatorTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:datahub,Test,PROD)");

  @Test
  public void testValidateAspectNumberUpsert() throws URISyntaxException {
    Urn propertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

    StructuredPropertyDefinition numberPropertyDef =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.number"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create(30.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(60.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(90.0)))));

    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties numberPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    boolean isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, numberPropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);

    assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(0.0)));
    numberPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, numberPropertyDef))
            .count(),
        1,
        "Should have raised exception for disallowed value 0.0");

    // Assign string value to number property
    StructuredPropertyValueAssignment stringAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("hello")));
    StructuredProperties stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(stringAssignment));

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(stringPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, numberPropertyDef))
            .count(),
        2,
        "Should have raised exception for mis-matched types `string` vs `number` && `hello` is not a valid value of [90.0, 30.0, 60.0]");
  }

  @Test
  public void testValidateAspectDateUpsert() throws URISyntaxException {
    Urn propertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

    // Assign string value
    StructuredPropertyValueAssignment stringAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("hello")));
    StructuredProperties stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(stringAssignment));

    // Assign invalid date
    StructuredPropertyDefinition datePropertyDef =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.date"));

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(stringPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, datePropertyDef))
            .count(),
        1,
        "Should have raised exception for mis-matched types");

    // Assign valid date
    StructuredPropertyValueAssignment dateAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(
                new PrimitivePropertyValueArray(PrimitivePropertyValue.create("2023-10-24")));
    StructuredProperties datePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(dateAssignment));

    boolean isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(datePayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, datePropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);
  }

  @Test
  public void testValidateAspectStringUpsert() throws URISyntaxException {
    Urn propertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

    // Assign string value
    StructuredPropertyValueAssignment stringAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("hello")));
    StructuredProperties stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(stringAssignment));

    // Assign date
    StructuredPropertyValueAssignment dateAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(
                new PrimitivePropertyValueArray(PrimitivePropertyValue.create("2023-10-24")));
    StructuredProperties datePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(dateAssignment));

    // Assign number
    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties numberPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    StructuredPropertyDefinition stringPropertyDef =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.string"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create("hello")),
                        new PropertyValue()
                            .setValue(PrimitivePropertyValue.create("2023-10-24")))));

    // Valid strings (both the date value and "hello" are valid)

    boolean isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(stringPayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, stringPropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);
    isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(datePayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, stringPropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);

    // Invalid: assign a number to the string property
    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, stringPropertyDef))
            .count(),
        2,
        "Should have raised exception for mis-matched types. The double 30.0 is not a `string` && not one of the allowed types `2023-10-24` or `hello`");

    // Invalid allowedValue

    assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("not hello")));
    stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(stringPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, stringPropertyDef))
            .count(),
        1,
        "Should have raised exception for disallowed value `not hello`");
  }

  @Test
  public void testValidateSoftDeletedUpsert() throws URISyntaxException {
    Urn propertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

    StructuredPropertyDefinition numberPropertyDef =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.number"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create(30.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(60.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(90.0)))));

    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties numberPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    boolean isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, numberPropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                new MockAspectRetriever(
                    propertyUrn, numberPropertyDef, new Status().setRemoved(true)))
            .count(),
        1,
        "Should have raised exception for soft deleted definition");
  }

  @Test
  public void testValidateImmutableMutation() throws URISyntaxException {
    Urn mutablePropertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.mutableProperty");
    StructuredPropertyDefinition mutablePropertyDef =
        new StructuredPropertyDefinition()
            .setImmutable(false)
            .setValueType(Urn.createFromString("urn:li:type:datahub.number"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create(30.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(60.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(90.0)))));
    StructuredPropertyValueAssignment mutableAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(mutablePropertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties mutablePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(mutableAssignment));

    Urn immutablePropertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.immutableProperty");
    StructuredPropertyDefinition immutablePropertyDef =
        new StructuredPropertyDefinition()
            .setImmutable(true)
            .setValueType(Urn.createFromString("urn:li:type:datahub.number"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create(30.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(60.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(90.0)))));
    StructuredPropertyValueAssignment immutableAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(immutablePropertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties immutablePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(immutableAssignment));

    // No previous values for either
    boolean noPreviousValid =
        StructuredPropertiesValidator.validateImmutable(
                    Stream.concat(
                            TestMCP.ofOneMCP(TEST_DATASET_URN, null, mutablePayload, TEST_REGISTRY)
                                .stream(),
                            TestMCP.ofOneMCP(
                                TEST_DATASET_URN, null, immutablePayload, TEST_REGISTRY)
                                .stream())
                        .collect(Collectors.toSet()),
                    new MockAspectRetriever(
                        Map.of(
                            mutablePropertyUrn,
                            List.of(mutablePropertyDef),
                            immutablePropertyUrn,
                            List.of(immutablePropertyDef))))
                .count()
            == 0;
    Assert.assertTrue(noPreviousValid);

    // Unchanged values of previous (no issues with immutability)
    boolean noChangeValid =
        StructuredPropertiesValidator.validateImmutable(
                    Stream.concat(
                            TestMCP.ofOneMCP(
                                TEST_DATASET_URN, mutablePayload, mutablePayload, TEST_REGISTRY)
                                .stream(),
                            TestMCP.ofOneMCP(
                                TEST_DATASET_URN, immutablePayload, immutablePayload, TEST_REGISTRY)
                                .stream())
                        .collect(Collectors.toSet()),
                    new MockAspectRetriever(
                        Map.of(
                            mutablePropertyUrn,
                            List.of(mutablePropertyDef),
                            immutablePropertyUrn,
                            List.of(immutablePropertyDef))))
                .count()
            == 0;
    Assert.assertTrue(noChangeValid);

    // invalid
    StructuredPropertyValueAssignment immutableAssignment2 =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(immutablePropertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(60.0)));
    StructuredProperties immutablePayload2 =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(immutableAssignment2));

    List<AspectValidationException> exceptions =
        StructuredPropertiesValidator.validateImmutable(
                Stream.concat(
                        TestMCP.ofOneMCP(
                            TEST_DATASET_URN, mutablePayload, mutablePayload, TEST_REGISTRY)
                            .stream(),
                        TestMCP.ofOneMCP(
                            TEST_DATASET_URN, immutablePayload, immutablePayload2, TEST_REGISTRY)
                            .stream())
                    .collect(Collectors.toSet()),
                new MockAspectRetriever(
                    Map.of(
                        mutablePropertyUrn,
                        List.of(mutablePropertyDef),
                        immutablePropertyUrn,
                        List.of(immutablePropertyDef))))
            .collect(Collectors.toList());

    Assert.assertEquals(exceptions.size(), 1, "Expected rejected mutation of immutable property.");
    Assert.assertEquals(exceptions.get(0).getAspectGroup().getKey(), TEST_DATASET_URN);
    Assert.assertTrue(
        exceptions.get(0).getMessage().contains("Cannot mutate an immutable property"));
  }

  @Test
  public void testValidateImmutableDelete() throws URISyntaxException {
    final StructuredProperties emptyProperties =
        new StructuredProperties().setProperties(new StructuredPropertyValueAssignmentArray());

    Urn mutablePropertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.mutableProperty");
    StructuredPropertyDefinition mutablePropertyDef =
        new StructuredPropertyDefinition()
            .setImmutable(false)
            .setValueType(Urn.createFromString("urn:li:type:datahub.number"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create(30.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(60.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(90.0)))));
    StructuredPropertyValueAssignment mutableAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(mutablePropertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties mutablePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(mutableAssignment));

    Urn immutablePropertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.immutableProperty");
    StructuredPropertyDefinition immutablePropertyDef =
        new StructuredPropertyDefinition()
            .setImmutable(true)
            .setValueType(Urn.createFromString("urn:li:type:datahub.number"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create(30.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(60.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(90.0)))));
    StructuredPropertyValueAssignment immutableAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(immutablePropertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties immutablePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(immutableAssignment));

    // Delete mutable, Delete with no-op for immutable allowed
    boolean noPreviousValid =
        StructuredPropertiesValidator.validateImmutable(
                    Stream.concat(
                            TestMCP.ofOneMCP(
                                TEST_DATASET_URN, mutablePayload, emptyProperties, TEST_REGISTRY)
                                .stream(),
                            TestMCP.ofOneMCP(
                                TEST_DATASET_URN, immutablePayload, immutablePayload, TEST_REGISTRY)
                                .stream())
                        // set to DELETE
                        .map(i -> ((TestMCP) i).toBuilder().changeType(ChangeType.DELETE).build())
                        .collect(Collectors.toSet()),
                    new MockAspectRetriever(
                        Map.of(
                            mutablePropertyUrn,
                            List.of(mutablePropertyDef),
                            immutablePropertyUrn,
                            List.of(immutablePropertyDef))))
                .count()
            == 0;
    Assert.assertTrue(noPreviousValid);

    // invalid (delete of mutable allowed, delete of immutable denied)
    List<AspectValidationException> exceptions =
        StructuredPropertiesValidator.validateImmutable(
                Stream.concat(
                        TestMCP.ofOneMCP(
                            TEST_DATASET_URN, mutablePayload, emptyProperties, TEST_REGISTRY)
                            .stream(),
                        TestMCP.ofOneMCP(
                            TEST_DATASET_URN, immutablePayload, emptyProperties, TEST_REGISTRY)
                            .stream())
                    // set to DELETE
                    .map(i -> ((TestMCP) i).toBuilder().changeType(ChangeType.DELETE).build())
                    .collect(Collectors.toSet()),
                new MockAspectRetriever(
                    Map.of(
                        mutablePropertyUrn,
                        List.of(mutablePropertyDef),
                        immutablePropertyUrn,
                        List.of(immutablePropertyDef))))
            .collect(Collectors.toList());

    Assert.assertEquals(exceptions.size(), 1, "Expected rejected delete of immutable property.");
    Assert.assertEquals(exceptions.get(0).getAspectGroup().getKey(), TEST_DATASET_URN);
    Assert.assertTrue(
        exceptions.get(0).getMessage().contains("Cannot delete an immutable property"));
  }
}
