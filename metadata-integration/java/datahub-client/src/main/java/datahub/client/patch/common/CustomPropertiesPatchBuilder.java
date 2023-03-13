package datahub.client.patch.common;

import com.fasterxml.jackson.databind.JsonNode;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.PatchOperationType;
import datahub.client.patch.subtypesupport.IntermediatePatchBuilder;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;


public class CustomPropertiesPatchBuilder<T extends AbstractMultiFieldPatchBuilder<T>> implements IntermediatePatchBuilder<T> {

  public static final String CUSTOM_PROPERTIES_BASE_PATH = "/customProperties/";

  private T parent;
  private final List<ImmutableTriple<String, String, JsonNode>> operations = new ArrayList<>();

  public CustomPropertiesPatchBuilder(T parentBuilder) {
    this.parent = parentBuilder;
  }

  public CustomPropertiesPatchBuilder<T> addProperty(String key, String value) {
    operations.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), CUSTOM_PROPERTIES_BASE_PATH + key,
        instance.textNode(value)));
    return this;
  }

  public CustomPropertiesPatchBuilder<T> removeProperty(String key, String value) {
    operations.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), CUSTOM_PROPERTIES_BASE_PATH + key,
        instance.textNode(value)));
    return this;
  }

  @Override
  public T getParent() {
    return parent;
  }

  @Override
  public List<ImmutableTriple<String, String, JsonNode>> getSubPaths() {
    return operations;
  }
}
