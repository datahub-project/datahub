package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CHART_INFO_ASPECT_NAME;
import static com.linkedin.metadata.aspect.patch.builder.PatchUtil.createEdgeValue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.builder.subtypesupport.CustomPropertiesPatchBuilderSupport;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class ChartInfoPatchBuilder extends AbstractMultiFieldPatchBuilder<ChartInfoPatchBuilder>
    implements CustomPropertiesPatchBuilderSupport<ChartInfoPatchBuilder> {
  private static final String INPUT_EDGES_PATH_START = "/inputEdges/";
  public static final String BASE_PATH = "/";
  public static final String DESCRIPTION_KEY = "description";
  public static final String TITLE_KEY = "title";
  public static final String EXTERNAL_URL_KEY = "externalUrl";
  public static final String CHART_URL_KEY = "chartUrl";
  public static final String LAST_REFRESHED_KEY = "lastRefreshed";
  public static final String TYPE_KEY = "type";
  public static final String ACCESS_KEY = "access";

  private CustomPropertiesPatchBuilder<ChartInfoPatchBuilder> customPropertiesPatchBuilder =
      new CustomPropertiesPatchBuilder<>(this);

  // Simplified with just Urn
  public ChartInfoPatchBuilder addInputEdge(@Nonnull Urn urn) {
    ObjectNode value = createEdgeValue(urn);

    pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), INPUT_EDGES_PATH_START + urn, value));
    return this;
  }

  public ChartInfoPatchBuilder removeInputEdge(@Nonnull Urn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), INPUT_EDGES_PATH_START + urn, null));
    return this;
  }

  public ChartInfoPatchBuilder setDescription(@Nullable String description) {
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

  public ChartInfoPatchBuilder setTitle(@Nullable String title) {
    if (title == null) {
      this.pathValues.add(
          ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + TITLE_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(), BASE_PATH + TITLE_KEY, instance.textNode(title)));
    }
    return this;
  }

  public ChartInfoPatchBuilder setExternalUrl(@Nullable String externalUrl) {
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

  public ChartInfoPatchBuilder setChartUrl(@Nullable String chartUrl) {
    if (chartUrl == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + CHART_URL_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + CHART_URL_KEY,
              instance.textNode(chartUrl)));
    }
    return this;
  }

  public ChartInfoPatchBuilder setLastRefreshed(@Nullable Long lastRefreshed) {
    if (lastRefreshed == null) {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.REMOVE.getValue(), BASE_PATH + LAST_REFRESHED_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + LAST_REFRESHED_KEY,
              instance.numberNode(lastRefreshed)));
    }
    return this;
  }

  public ChartInfoPatchBuilder setType(@Nullable com.linkedin.chart.ChartType type) {
    if (type == null) {
      this.pathValues.add(
          ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + TYPE_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + TYPE_KEY,
              instance.textNode(type.name())));
    }
    return this;
  }

  public ChartInfoPatchBuilder setAccess(@Nullable com.linkedin.common.AccessLevel access) {
    if (access == null) {
      this.pathValues.add(
          ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + ACCESS_KEY, null));
    } else {
      this.pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(),
              BASE_PATH + ACCESS_KEY,
              instance.textNode(access.name())));
    }
    return this;
  }

  @Override
  public ChartInfoPatchBuilder addCustomProperty(@Nonnull String key, @Nonnull String value) {
    this.customPropertiesPatchBuilder.addProperty(key, value);
    return this;
  }

  @Override
  public ChartInfoPatchBuilder removeCustomProperty(@Nonnull String key) {
    this.customPropertiesPatchBuilder.removeProperty(key);
    return this;
  }

  @Override
  public ChartInfoPatchBuilder setCustomProperties(Map<String, String> properties) {
    customPropertiesPatchBuilder.setProperties(properties);
    return this;
  }

  @Override
  protected String getAspectName() {
    return CHART_INFO_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return CHART_ENTITY_NAME;
  }
}
