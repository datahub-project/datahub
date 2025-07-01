package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_INFO_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.builder.subtypesupport.CustomPropertiesPatchBuilderSupport;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DataJobInfoPatchBuilder extends AbstractMultiFieldPatchBuilder<DataJobInfoPatchBuilder>
    implements CustomPropertiesPatchBuilderSupport<DataJobInfoPatchBuilder> {

  public static final String BASE_PATH = "/";

  public static final String NAME_KEY = "name";
  public static final String DESCRIPTION_KEY = "description";
  public static final String FLOW_URN_KEY = "flowUrn";
  public static final String CREATED_KEY = "created";
  public static final String LAST_MODIFIED_KEY = "lastModified";
  public static final String TIME_KEY = "time";
  public static final String ACTOR_KEY = "actor";
  public static final String TYPE_KEY = "type";
  public static final String CUSTOM_PROPERTIES_KEY = "customProperties";

  private CustomPropertiesPatchBuilder<DataJobInfoPatchBuilder> customPropertiesPatchBuilder =
      new CustomPropertiesPatchBuilder<>(this);

  public DataJobInfoPatchBuilder setName(@Nonnull String name) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + NAME_KEY, instance.textNode(name)));
    return this;
  }

  public DataJobInfoPatchBuilder setDescription(@Nullable String description) {
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

  public DataJobInfoPatchBuilder setType(@Nonnull String type) {
    ObjectNode union = instance.objectNode();
    union.set("string", instance.textNode(type));
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), BASE_PATH + TYPE_KEY, union));
    return this;
  }

  public DataJobInfoPatchBuilder setFlowUrn(@Nullable DataFlowUrn flowUrn) {
    if (flowUrn == null) {
      pathValues.add(
          ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + FLOW_URN_KEY, null));
    } else {
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + FLOW_URN_KEY,
              instance.textNode(flowUrn.toString())));
    }
    return this;
  }

  public DataJobInfoPatchBuilder setCreated(@Nullable TimeStamp created) {
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

  public DataJobInfoPatchBuilder setLastModified(@Nullable TimeStamp lastModified) {
    if (lastModified == null) {
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + LAST_MODIFIED_KEY, null));
    } else {
      ObjectNode lastModifiedNode = instance.objectNode();
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
    return DATA_JOB_INFO_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATA_JOB_ENTITY_NAME;
  }

  @Override
  public DataJobInfoPatchBuilder addCustomProperty(@Nonnull String key, @Nonnull String value) {
    customPropertiesPatchBuilder.addProperty(key, value);
    return this;
  }

  @Override
  public DataJobInfoPatchBuilder removeCustomProperty(@Nonnull String key) {
    customPropertiesPatchBuilder.removeProperty(key);
    return this;
  }

  @Override
  public DataJobInfoPatchBuilder setCustomProperties(Map<String, String> properties) {
    customPropertiesPatchBuilder.setProperties(properties);
    return this;
  }
}
