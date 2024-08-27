package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class StructuredPropertiesPatchBuilder
    extends AbstractMultiFieldPatchBuilder<StructuredPropertiesPatchBuilder> {

  private static final String BASE_PATH = "/properties";
  private static final String URN_KEY = "propertyUrn";
  private static final String VALUES_KEY = "values";

  /**
   * Remove a property from a structured properties aspect. If the property doesn't exist, this is a
   * no-op.
   *
   * @param propertyUrn
   * @return
   */
  public StructuredPropertiesPatchBuilder removeProperty(Urn propertyUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + "/" + propertyUrn, null));
    return this;
  }

  public StructuredPropertiesPatchBuilder setNumberProperty(
      @Nonnull Urn propertyUrn, @Nullable Integer propertyValue) {
    ObjectNode newProperty = instance.objectNode();
    newProperty.put(URN_KEY, propertyUrn.toString());

    ArrayNode valuesNode = instance.arrayNode();
    ObjectNode propertyValueNode = instance.objectNode();
    propertyValueNode.set("double", instance.numberNode(propertyValue));
    valuesNode.add(propertyValueNode);
    newProperty.set(VALUES_KEY, valuesNode);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + "/" + propertyUrn, newProperty));
    return this;
  }

  public StructuredPropertiesPatchBuilder setNumberProperty(
      @Nonnull Urn propertyUrn, @Nonnull List<Integer> propertyValues) {
    ObjectNode newProperty = instance.objectNode();
    newProperty.put(URN_KEY, propertyUrn.toString());

    ArrayNode valuesNode = instance.arrayNode();
    propertyValues.forEach(
        propertyValue -> {
          ObjectNode propertyValueNode = instance.objectNode();
          propertyValueNode.set("double", instance.numberNode(propertyValue));
          valuesNode.add(propertyValueNode);
        });
    newProperty.set(VALUES_KEY, valuesNode);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + "/" + propertyUrn, newProperty));
    return this;
  }

  public StructuredPropertiesPatchBuilder setStringProperty(
      @Nonnull Urn propertyUrn, @Nullable String propertyValue) {
    ObjectNode newProperty = instance.objectNode();
    newProperty.put(URN_KEY, propertyUrn.toString());

    ArrayNode valuesNode = instance.arrayNode();
    ObjectNode propertyValueNode = instance.objectNode();
    propertyValueNode.set("string", instance.textNode(propertyValue));
    valuesNode.add(propertyValueNode);
    newProperty.set(VALUES_KEY, valuesNode);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + "/" + propertyUrn, newProperty));
    return this;
  }

  public StructuredPropertiesPatchBuilder setStringProperty(
      @Nonnull Urn propertyUrn, @Nonnull List<String> propertyValues) {
    ObjectNode newProperty = instance.objectNode();
    newProperty.put(URN_KEY, propertyUrn.toString());

    ArrayNode valuesNode = instance.arrayNode();
    propertyValues.forEach(
        propertyValue -> {
          ObjectNode propertyValueNode = instance.objectNode();
          propertyValueNode.set("string", instance.textNode(propertyValue));
          valuesNode.add(propertyValueNode);
        });
    newProperty.set(VALUES_KEY, valuesNode);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + "/" + propertyUrn, newProperty));
    return this;
  }

  @Override
  protected String getAspectName() {
    return STRUCTURED_PROPERTIES_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    if (this.targetEntityUrn == null) {
      throw new IllegalStateException(
          "Target Entity Urn must be set to determine entity type before building Patch.");
    }
    return this.targetEntityUrn.getEntityType();
  }
}
