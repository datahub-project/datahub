package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DATA_FLOW_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_FLOW_INFO_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.TimeStamp;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.builder.subtypesupport.CustomPropertiesPatchBuilderSupport;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DataFlowInfoPatchBuilder
    extends AbstractMultiFieldPatchBuilder<DataFlowInfoPatchBuilder>
    implements CustomPropertiesPatchBuilderSupport<DataFlowInfoPatchBuilder> {

  public static final String BASE_PATH = "/";

  public static final String NAME_KEY = "name";
  public static final String DESCRIPTION_KEY = "description";
  public static final String PROJECT_KEY = "project";
  public static final String CREATED_KEY = "created";
  public static final String LAST_MODIFIED_KEY = "lastModified";
  public static final String TIME_KEY = "time";
  public static final String ACTOR_KEY = "actor";

  private CustomPropertiesPatchBuilder<DataFlowInfoPatchBuilder> customPropertiesPatchBuilder =
      new CustomPropertiesPatchBuilder<>(this);

  public DataFlowInfoPatchBuilder setName(@Nonnull String name) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + NAME_KEY, instance.textNode(name)));
    return this;
  }

  public DataFlowInfoPatchBuilder setDescription(@Nullable String description) {
    if (description == null) {
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + DESCRIPTION_KEY, null));
    } else {
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + DESCRIPTION_KEY,
              instance.textNode(description)));
    }
    return this;
  }

  public DataFlowInfoPatchBuilder setProject(@Nullable String project) {
    if (project == null) {
      pathValues.add(
          ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + PROJECT_KEY, null));
    } else {
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + PROJECT_KEY,
              instance.textNode(project)));
    }
    return this;
  }

  public DataFlowInfoPatchBuilder setCreated(@Nullable TimeStamp created) {

    if (created == null) {
      pathValues.add(
          ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + CREATED_KEY, null));
    } else {
      ObjectNode createdNode = instance.objectNode();
      createdNode.put(TIME_KEY, created.getTime());
      if (created.getActor() != null) {
        createdNode.put(ACTOR_KEY, created.getActor().toString());
      }
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(), BASE_PATH + CREATED_KEY, createdNode));
    }
    return this;
  }

  public DataFlowInfoPatchBuilder setLastModified(@Nullable TimeStamp lastModified) {
    ObjectNode lastModifiedNode = instance.objectNode();
    if (lastModified == null) {
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + LAST_MODIFIED_KEY, null));
    } else {
      lastModifiedNode.put(TIME_KEY, lastModified.getTime());
      if (lastModified.getActor() != null) {
        lastModifiedNode.put(ACTOR_KEY, lastModified.getActor().toString());
      }
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(), BASE_PATH + LAST_MODIFIED_KEY, lastModifiedNode));
    }
    return this;
  }

  @Override
  protected String getAspectName() {
    return DATA_FLOW_INFO_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATA_FLOW_ENTITY_NAME;
  }

  @Override
  public DataFlowInfoPatchBuilder addCustomProperty(@Nonnull String key, @Nonnull String value) {
    customPropertiesPatchBuilder.addProperty(key, value);
    return this;
  }

  @Override
  public DataFlowInfoPatchBuilder removeCustomProperty(@Nonnull String key) {
    customPropertiesPatchBuilder.removeProperty(key);
    return this;
  }

  @Override
  public DataFlowInfoPatchBuilder setCustomProperties(Map<String, String> properties) {
    customPropertiesPatchBuilder.setProperties(properties);
    return this;
  }
}
