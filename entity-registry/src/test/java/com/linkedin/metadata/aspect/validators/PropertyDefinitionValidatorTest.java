package com.linkedin.metadata.aspect.validators;

import static org.testng.Assert.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.PropertyDefinitionValidator;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class PropertyDefinitionValidatorTest {
  @Test
  public void testValidatePreCommitNoPrevious()
      throws URISyntaxException, AspectValidationException {
    StructuredPropertyDefinition newProperty = new StructuredPropertyDefinition();
    newProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    newProperty.setDisplayName("newProp");
    newProperty.setQualifiedName("prop3");
    newProperty.setCardinality(PropertyCardinality.MULTIPLE);
    newProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    assertTrue(PropertyDefinitionValidator.validate(null, newProperty));
  }

  @Test
  public void testCanChangeSingleToMultiple()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.SINGLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setCardinality(PropertyCardinality.MULTIPLE);
    assertTrue(PropertyDefinitionValidator.validate(oldProperty, newProperty));
  }

  @Test
  public void testCannotChangeMultipleToSingle()
      throws URISyntaxException, CloneNotSupportedException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setCardinality(PropertyCardinality.SINGLE);
    assertThrows(
        AspectValidationException.class,
        () -> PropertyDefinitionValidator.validate(oldProperty, newProperty));
  }

  @Test
  public void testCannotChangeValueType() throws URISyntaxException, CloneNotSupportedException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setValueType(Urn.createFromString("urn:li:logicalType:NUMBER"));
    assertThrows(
        AspectValidationException.class,
        () -> PropertyDefinitionValidator.validate(oldProperty, newProperty));
  }

  @Test
  public void testCanChangeDisplayName()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setDisplayName("newProp");
    assertTrue(PropertyDefinitionValidator.validate(oldProperty, newProperty));
  }

  @Test
  public void testCannotChangeFullyQualifiedName()
      throws URISyntaxException, CloneNotSupportedException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setQualifiedName("newProp");
    assertThrows(
        AspectValidationException.class,
        () -> PropertyDefinitionValidator.validate(oldProperty, newProperty));
  }

  @Test
  public void testCannotChangeRestrictAllowedValues()
      throws URISyntaxException, CloneNotSupportedException {
    // No constraint -> constraint case
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    PropertyValue allowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(1.0)).setDescription("hello");
    newProperty.setAllowedValues(new PropertyValueArray(allowedValue));
    assertThrows(
        AspectValidationException.class,
        () -> PropertyDefinitionValidator.validate(oldProperty, newProperty));

    // Remove allowed values from constraint case
    PropertyValue oldAllowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(3.0)).setDescription("hello");
    oldProperty.setAllowedValues((new PropertyValueArray(allowedValue, oldAllowedValue)));
    assertThrows(
        AspectValidationException.class,
        () -> PropertyDefinitionValidator.validate(oldProperty, newProperty));
  }

  @Test
  public void testCanExpandAllowedValues()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    // Constraint -> no constraint case
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    PropertyValue allowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(1.0)).setDescription("hello");
    oldProperty.setAllowedValues(new PropertyValueArray(allowedValue));
    assertTrue(PropertyDefinitionValidator.validate(oldProperty, newProperty));

    // Add allowed values to constraint case
    PropertyValue newAllowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(3.0)).setDescription("hello");
    newProperty.setAllowedValues((new PropertyValueArray(allowedValue, newAllowedValue)));
    assertTrue(PropertyDefinitionValidator.validate(oldProperty, newProperty));
  }

  @Test
  public void testCanChangeAllowedValueDescriptions()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    // Constraint -> no constraint case
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    PropertyValue allowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(1.0)).setDescription("hello");
    oldProperty.setAllowedValues(new PropertyValueArray(allowedValue));
    PropertyValue newAllowedValue =
        new PropertyValue()
            .setValue(PrimitivePropertyValue.create(1.0))
            .setDescription("hello there");
    newProperty.setAllowedValues(new PropertyValueArray(newAllowedValue));
    assertTrue(PropertyDefinitionValidator.validate(oldProperty, newProperty));
  }
}
