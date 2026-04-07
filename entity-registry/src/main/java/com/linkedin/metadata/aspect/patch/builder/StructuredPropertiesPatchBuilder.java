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

  private static final String BASE_PATH = "/properties/";
  private static final String URN_KEY = "propertyUrn";
  private static final String VALUES_KEY = "values";

  /** Removes all entries for this property URN across every attribution source. */
  public StructuredPropertiesPatchBuilder removeProperty(@Nonnull Urn propertyUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValueUrn(propertyUrn), null));
    return this;
  }

  /** Removes only the entry for this property URN attributed to a specific source. */
  public StructuredPropertiesPatchBuilder removeProperty(
      @Nonnull Urn propertyUrn, @Nonnull Urn attributionSource) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH
                + encodeValueUrn(propertyUrn)
                + "/"
                + encodeValue(attributionSource.toString()),
            null));
    return this;
  }

  public StructuredPropertiesPatchBuilder setNumberProperty(
      @Nonnull Urn propertyUrn, @Nullable Integer propertyValue) {
    return setNumberProperty(propertyUrn, propertyValue, null);
  }

  public StructuredPropertiesPatchBuilder setNumberProperty(
      @Nonnull Urn propertyUrn, @Nullable Integer propertyValue, @Nullable Urn attributionSource) {
    ObjectNode newProperty = instance.objectNode();
    newProperty.put(URN_KEY, propertyUrn.toString());
    ArrayNode valuesNode = instance.arrayNode();
    valuesNode.add(instance.objectNode().set("double", instance.numberNode(propertyValue)));
    newProperty.set(VALUES_KEY, valuesNode);
    String sourcePath = attributionSource != null ? encodeValue(attributionSource.toString()) : "";
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValueUrn(propertyUrn) + "/" + sourcePath,
            newProperty));
    return this;
  }

  public StructuredPropertiesPatchBuilder setNumberProperty(
      @Nonnull Urn propertyUrn, @Nonnull List<Integer> propertyValues) {
    return setNumberProperty(propertyUrn, propertyValues, null);
  }

  public StructuredPropertiesPatchBuilder setNumberProperty(
      @Nonnull Urn propertyUrn,
      @Nonnull List<Integer> propertyValues,
      @Nullable Urn attributionSource) {
    ObjectNode newProperty = instance.objectNode();
    newProperty.put(URN_KEY, propertyUrn.toString());
    ArrayNode valuesNode = instance.arrayNode();
    propertyValues.forEach(
        v -> valuesNode.add(instance.objectNode().set("double", instance.numberNode(v))));
    newProperty.set(VALUES_KEY, valuesNode);
    String sourcePath = attributionSource != null ? encodeValue(attributionSource.toString()) : "";
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValueUrn(propertyUrn) + "/" + sourcePath,
            newProperty));
    return this;
  }

  public StructuredPropertiesPatchBuilder setStringProperty(
      @Nonnull Urn propertyUrn, @Nullable String propertyValue) {
    return setStringProperty(propertyUrn, propertyValue, null);
  }

  public StructuredPropertiesPatchBuilder setStringProperty(
      @Nonnull Urn propertyUrn, @Nullable String propertyValue, @Nullable Urn attributionSource) {
    ObjectNode newProperty = instance.objectNode();
    newProperty.put(URN_KEY, propertyUrn.toString());
    ArrayNode valuesNode = instance.arrayNode();
    valuesNode.add(instance.objectNode().set("string", instance.textNode(propertyValue)));
    newProperty.set(VALUES_KEY, valuesNode);
    String sourcePath = attributionSource != null ? encodeValue(attributionSource.toString()) : "";
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValueUrn(propertyUrn) + "/" + sourcePath,
            newProperty));
    return this;
  }

  public StructuredPropertiesPatchBuilder setStringProperty(
      @Nonnull Urn propertyUrn, @Nonnull List<String> propertyValues) {
    return setStringProperty(propertyUrn, propertyValues, null);
  }

  public StructuredPropertiesPatchBuilder setStringProperty(
      @Nonnull Urn propertyUrn,
      @Nonnull List<String> propertyValues,
      @Nullable Urn attributionSource) {
    ObjectNode newProperty = instance.objectNode();
    newProperty.put(URN_KEY, propertyUrn.toString());
    ArrayNode valuesNode = instance.arrayNode();
    propertyValues.forEach(
        v -> valuesNode.add(instance.objectNode().set("string", instance.textNode(v))));
    newProperty.set(VALUES_KEY, valuesNode);
    String sourcePath = attributionSource != null ? encodeValue(attributionSource.toString()) : "";
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValueUrn(propertyUrn) + "/" + sourcePath,
            newProperty));
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
