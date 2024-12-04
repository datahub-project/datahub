package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;

import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.builder.subtypesupport.CustomPropertiesPatchBuilderSupport;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DatasetPropertiesPatchBuilder
    extends AbstractMultiFieldPatchBuilder<DatasetPropertiesPatchBuilder>
    implements CustomPropertiesPatchBuilderSupport<DatasetPropertiesPatchBuilder> {

  public static final String BASE_PATH = "/";

  public static final String DESCRIPTION_KEY = "description";
  public static final String EXTERNAL_URL_KEY = "externalUrl";
  public static final String NAME_KEY = "name";
  public static final String QUALIFIED_NAME_KEY = "qualifiedName";
  public static final String URI_KEY = "uri";

  private CustomPropertiesPatchBuilder<DatasetPropertiesPatchBuilder> customPropertiesPatchBuilder =
      new CustomPropertiesPatchBuilder<>(this);

  public DatasetPropertiesPatchBuilder setExternalUrl(@Nullable String externalUrl) {
    if (externalUrl == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + EXTERNAL_URL_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + EXTERNAL_URL_KEY,
              instance.textNode(externalUrl)));
    }
    return this;
  }

  public DatasetPropertiesPatchBuilder setName(@Nullable String name) {
    if (name == null) {
      this.pathValues.add(
          ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + NAME_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(), BASE_PATH + NAME_KEY, instance.textNode(name)));
    }
    return this;
  }

  public DatasetPropertiesPatchBuilder setQualifiedName(@Nullable String qualifiedName) {
    if (qualifiedName == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + QUALIFIED_NAME_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + QUALIFIED_NAME_KEY,
              instance.textNode(qualifiedName)));
    }
    return this;
  }

  public DatasetPropertiesPatchBuilder setDescription(@Nullable String description) {
    if (description == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + DESCRIPTION_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + DESCRIPTION_KEY,
              instance.textNode(description)));
    }
    return this;
  }

  public DatasetPropertiesPatchBuilder setUri(@Nullable String uri) {
    if (uri == null) {
      this.pathValues.add(
          ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + URI_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(), BASE_PATH + URI_KEY, instance.textNode(uri)));
    }
    return this;
  }

  @Override
  public DatasetPropertiesPatchBuilder addCustomProperty(
      @Nonnull String key, @Nonnull String value) {
    this.customPropertiesPatchBuilder.addProperty(key, value);
    return this;
  }

  @Override
  public DatasetPropertiesPatchBuilder removeCustomProperty(@Nonnull String key) {
    this.customPropertiesPatchBuilder.removeProperty(key);
    return this;
  }

  @Override
  public DatasetPropertiesPatchBuilder setCustomProperties(Map<String, String> properties) {
    customPropertiesPatchBuilder.setProperties(properties);
    return this;
  }

  @Override
  protected String getAspectName() {
    return DATASET_PROPERTIES_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return DATASET_ENTITY_NAME;
  }
}
