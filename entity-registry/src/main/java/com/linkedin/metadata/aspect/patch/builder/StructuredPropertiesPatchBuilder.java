package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class StructuredPropertiesPatchBuilder
    extends AbstractMultiFieldPatchBuilder<StructuredPropertiesPatchBuilder> {

  private static final String BASE_PATH = "/properties";
  private static final String URN_KEY = "urn";
  private static final String CONTEXT_KEY = "context";

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

  public StructuredPropertiesPatchBuilder setProperty(
      @Nonnull Urn propertyUrn, @Nullable List<Object> propertyValues) {
    propertyValues.stream()
        .map(
            propertyValue ->
                propertyValue instanceof Integer
                    ? this.setProperty(propertyUrn, (Integer) propertyValue)
                    : this.setProperty(propertyUrn, String.valueOf(propertyValue)))
        .collect(Collectors.toList());
    return this;
  }

  public StructuredPropertiesPatchBuilder setProperty(
      @Nonnull Urn propertyUrn, @Nullable Integer propertyValue) {
    ValueNode propertyValueNode = instance.numberNode((Integer) propertyValue);
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, propertyUrn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + "/" + propertyUrn, propertyValueNode));
    return this;
  }

  public StructuredPropertiesPatchBuilder setProperty(
      @Nonnull Urn propertyUrn, @Nullable String propertyValue) {
    ValueNode propertyValueNode = instance.textNode(String.valueOf(propertyValue));
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, propertyUrn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + "/" + propertyUrn, propertyValueNode));
    return this;
  }

  public StructuredPropertiesPatchBuilder addProperty(
      @Nonnull Urn propertyUrn, @Nullable Integer propertyValue) {
    ValueNode propertyValueNode = instance.numberNode((Integer) propertyValue);
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, propertyUrn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + "/" + propertyUrn + "/" + String.valueOf(propertyValue),
            propertyValueNode));
    return this;
  }

  public StructuredPropertiesPatchBuilder addProperty(
      @Nonnull Urn propertyUrn, @Nullable String propertyValue) {
    ValueNode propertyValueNode = instance.textNode(String.valueOf(propertyValue));
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, propertyUrn.toString());
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + "/" + propertyUrn + "/" + String.valueOf(propertyValue),
            propertyValueNode));
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
