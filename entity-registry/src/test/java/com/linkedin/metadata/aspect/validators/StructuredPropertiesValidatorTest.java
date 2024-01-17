package com.linkedin.metadata.aspect.validators;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.StructuredPropertiesValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StructuredPropertiesValidatorTest {

  static class MockAspectRetriever implements AspectRetriever {
    StructuredPropertyDefinition _propertyDefinition;

    MockAspectRetriever(StructuredPropertyDefinition defToReturn) {
      this._propertyDefinition = defToReturn;
    }

    @Nonnull
    @Override
    public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
        Set<Urn> urns, Set<String> aspectNames)
        throws RemoteInvocationException, URISyntaxException {
      return Map.of(
          urns.stream().findFirst().get(),
          Map.of(aspectNames.stream().findFirst().get(), new Aspect(_propertyDefinition.data())));
    }

    @Nonnull
    @Override
    public EntityRegistry getEntityRegistry() {
      return null;
    }
  }

  @Test
  public void testValidateAspectNumberUpsert() throws URISyntaxException {
    StructuredPropertyDefinition numberPropertyDef =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.number"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create(30.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(60.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(90.0)))));

    try {
      StructuredPropertyValueAssignment assignment =
          new StructuredPropertyValueAssignment()
              .setPropertyUrn(
                  Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
              .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
      StructuredProperties numberPayload =
          new StructuredProperties()
              .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

      boolean isValid =
          StructuredPropertiesValidator.validate(
              numberPayload, new MockAspectRetriever(numberPropertyDef));
      Assert.assertTrue(isValid);
    } catch (AspectValidationException e) {
      throw new RuntimeException(e);
    }

    try {
      StructuredPropertyValueAssignment assignment =
          new StructuredPropertyValueAssignment()
              .setPropertyUrn(
                  Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
              .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(0.0)));
      StructuredProperties numberPayload =
          new StructuredProperties()
              .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

      StructuredPropertiesValidator.validate(
          numberPayload, new MockAspectRetriever(numberPropertyDef));
      Assert.fail("Should have raised exception for disallowed value 0.0");
    } catch (AspectValidationException e) {
      Assert.assertTrue(e.getMessage().contains("{double=0.0} should be one of [{"));
    }

    // Assign string value to number property
    StructuredPropertyValueAssignment stringAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("hello")));
    StructuredProperties stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(stringAssignment));
    try {
      StructuredPropertiesValidator.validate(
          stringPayload, new MockAspectRetriever(numberPropertyDef));
      Assert.fail("Should have raised exception for mis-matched types");
    } catch (AspectValidationException e) {
      Assert.assertTrue(e.getMessage().contains("should be a number"));
    }
  }

  @Test
  public void testValidateAspectDateUpsert() throws URISyntaxException {
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
    try {
      StructuredPropertiesValidator.validate(
          stringPayload, new MockAspectRetriever(datePropertyDef));
      Assert.fail("Should have raised exception for mis-matched types");
    } catch (AspectValidationException e) {
      Assert.assertTrue(e.getMessage().contains("should be a date with format"));
    }

    // Assign valid date
    StructuredPropertyValueAssignment dateAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(
                new PrimitivePropertyValueArray(PrimitivePropertyValue.create("2023-10-24")));
    StructuredProperties datePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(dateAssignment));
    try {
      boolean isValid =
          StructuredPropertiesValidator.validate(
              datePayload, new MockAspectRetriever(datePropertyDef));
      Assert.assertTrue(isValid);
    } catch (AspectValidationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testValidateAspectStringUpsert() throws URISyntaxException {
    // Assign string value
    StructuredPropertyValueAssignment stringAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("hello")));
    StructuredProperties stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(stringAssignment));

    // Assign date
    StructuredPropertyValueAssignment dateAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(
                new PrimitivePropertyValueArray(PrimitivePropertyValue.create("2023-10-24")));
    StructuredProperties datePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(dateAssignment));

    // Assign number
    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
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
    try {
      boolean isValid =
          StructuredPropertiesValidator.validate(
              stringPayload, new MockAspectRetriever(stringPropertyDef));
      Assert.assertTrue(isValid);
      isValid =
          StructuredPropertiesValidator.validate(
              datePayload, new MockAspectRetriever(stringPropertyDef));
      Assert.assertTrue(isValid);
    } catch (AspectValidationException e) {
      throw new RuntimeException(e);
    }

    // Invalid: assign a number to the string property
    try {
      StructuredPropertiesValidator.validate(
          numberPayload, new MockAspectRetriever(stringPropertyDef));
      Assert.fail("Should have raised exception for mis-matched types");
    } catch (AspectValidationException e) {
      Assert.assertTrue(e.getMessage().contains("should be a string"));
    }

    // Invalid allowedValue
    try {
      assignment =
          new StructuredPropertyValueAssignment()
              .setPropertyUrn(
                  Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
              .setValues(
                  new PrimitivePropertyValueArray(PrimitivePropertyValue.create("not hello")));
      stringPayload =
          new StructuredProperties()
              .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

      StructuredPropertiesValidator.validate(
          stringPayload, new MockAspectRetriever(stringPropertyDef));
      Assert.fail("Should have raised exception for disallowed value `not hello`");
    } catch (AspectValidationException e) {
      Assert.assertTrue(e.getMessage().contains("{string=not hello} should be one of [{"));
    }
  }
}
