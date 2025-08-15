package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.builder.subtypesupport.IntermediatePatchBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class CustomPropertiesPatchBuilder<T extends AbstractMultiFieldPatchBuilder<T>>
    implements IntermediatePatchBuilder<T> {

  public static final String CUSTOM_PROPERTIES_BASE_PATH = "/customProperties";

  private final T parent;
  private final List<ImmutableTriple<String, String, JsonNode>> operations;

  public CustomPropertiesPatchBuilder(T parentBuilder) {
    this.parent = parentBuilder;
    if (parentBuilder != null) {
      // If a parent builder is provided, we use the same path operations list.
      this.operations = parentBuilder.getPathValues();
    } else {
      this.operations = new ArrayList<>();
    }
  }

  /**
   * Add a property to a custom properties field
   *
   * @param key
   * @param value
   * @return
   */
  public CustomPropertiesPatchBuilder<T> addProperty(String key, String value) {
    operations.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            CUSTOM_PROPERTIES_BASE_PATH + "/" + key,
            instance.textNode(value)));
    return this;
  }

  /**
   * Remove a property from a custom properties field. If the property doesn't exist, this is a
   * no-op.
   *
   * @param key
   * @return
   */
  public CustomPropertiesPatchBuilder<T> removeProperty(String key) {
    operations.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), CUSTOM_PROPERTIES_BASE_PATH + "/" + key, null));
    return this;
  }

  /**
   * Fully replace the properties of the target aspect
   *
   * @param properties
   * @return
   */
  public CustomPropertiesPatchBuilder<T> setProperties(Map<String, String> properties) {
    ObjectNode propertiesNode = instance.objectNode();
    properties.forEach((key, value) -> propertiesNode.set(key, instance.textNode(value)));
    operations.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), CUSTOM_PROPERTIES_BASE_PATH, propertiesNode));
    return this;
  }

  @Override
  public T getParent() {
    return parent;
  }
}
