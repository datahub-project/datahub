package com.linkedin.metadata.models.registry;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.models.registry.EntityRegistryUtils.*;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.metadata.aspect.patch.template.chart.ChartInfoTemplate;
import com.linkedin.metadata.aspect.patch.template.common.GlobalTagsTemplate;
import com.linkedin.metadata.aspect.patch.template.common.GlossaryTermsTemplate;
import com.linkedin.metadata.aspect.patch.template.common.OwnershipTemplate;
import com.linkedin.metadata.aspect.patch.template.common.StructuredPropertiesTemplate;
import com.linkedin.metadata.aspect.patch.template.common.VersionPropertiesTemplate;
import com.linkedin.metadata.aspect.patch.template.dashboard.DashboardInfoTemplate;
import com.linkedin.metadata.aspect.patch.template.dataflow.DataFlowInfoTemplate;
import com.linkedin.metadata.aspect.patch.template.datajob.DataJobInfoTemplate;
import com.linkedin.metadata.aspect.patch.template.datajob.DataJobInputOutputTemplate;
import com.linkedin.metadata.aspect.patch.template.dataproduct.DataProductPropertiesTemplate;
import com.linkedin.metadata.aspect.patch.template.dataset.DatasetPropertiesTemplate;
import com.linkedin.metadata.aspect.patch.template.dataset.EditableSchemaMetadataTemplate;
import com.linkedin.metadata.aspect.patch.template.dataset.UpstreamLineageTemplate;
import com.linkedin.metadata.aspect.patch.template.form.FormInfoTemplate;
import com.linkedin.metadata.aspect.patch.template.structuredproperty.StructuredPropertyDefinitionTemplate;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Implementation of {@link EntityRegistry} that builds {@link DefaultEntitySpec} objects from the a
 * {@link Snapshot} Record Template present on the classpath
 */
public class SnapshotEntityRegistry implements EntityRegistry {

  private final Map<String, EntitySpec> entityNameToSpec;
  private final List<EntitySpec> entitySpecs;
  private final AspectTemplateEngine _aspectTemplateEngine;
  private final Map<String, AspectSpec> _aspectNameToSpec;

  @Getter @Nullable
  private BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider;

  private static final SnapshotEntityRegistry INSTANCE = new SnapshotEntityRegistry();

  public SnapshotEntityRegistry() {
    entityNameToSpec =
        new EntitySpecBuilder()
            .buildEntitySpecs(new Snapshot().schema()).stream()
                .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
    entitySpecs = new ArrayList<>(entityNameToSpec.values());
    _aspectNameToSpec = populateAspectMap(entitySpecs);
    _aspectTemplateEngine = populateTemplateEngine(_aspectNameToSpec);
    pluginFactoryProvider = null;
  }

  public SnapshotEntityRegistry(
      BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider) {
    entityNameToSpec =
        new EntitySpecBuilder()
            .buildEntitySpecs(new Snapshot().schema()).stream()
                .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
    entitySpecs = new ArrayList<>(entityNameToSpec.values());
    _aspectNameToSpec = populateAspectMap(entitySpecs);
    _aspectTemplateEngine = populateTemplateEngine(_aspectNameToSpec);
    this.pluginFactoryProvider = pluginFactoryProvider;
  }

  public SnapshotEntityRegistry(UnionTemplate snapshot) {
    entityNameToSpec =
        new EntitySpecBuilder()
            .buildEntitySpecs(snapshot.schema()).stream()
                .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
    entitySpecs = new ArrayList<>(entityNameToSpec.values());
    _aspectNameToSpec = populateAspectMap(entitySpecs);
    _aspectTemplateEngine = populateTemplateEngine(_aspectNameToSpec);
  }

  private AspectTemplateEngine populateTemplateEngine(Map<String, AspectSpec> aspectSpecs) {
    // TODO: This should be more dynamic ideally, "hardcoding" for now, passing in aspect spec map
    // preemptively

    Map<String, Template<? extends RecordTemplate>> aspectSpecTemplateMap = new HashMap<>();
    aspectSpecTemplateMap.put(OWNERSHIP_ASPECT_NAME, new OwnershipTemplate());
    aspectSpecTemplateMap.put(DATASET_PROPERTIES_ASPECT_NAME, new DatasetPropertiesTemplate());
    aspectSpecTemplateMap.put(UPSTREAM_LINEAGE_ASPECT_NAME, new UpstreamLineageTemplate());
    aspectSpecTemplateMap.put(GLOBAL_TAGS_ASPECT_NAME, new GlobalTagsTemplate());
    aspectSpecTemplateMap.put(
        EDITABLE_SCHEMA_METADATA_ASPECT_NAME, new EditableSchemaMetadataTemplate());
    aspectSpecTemplateMap.put(GLOSSARY_TERMS_ASPECT_NAME, new GlossaryTermsTemplate());
    aspectSpecTemplateMap.put(DATA_FLOW_INFO_ASPECT_NAME, new DataFlowInfoTemplate());
    aspectSpecTemplateMap.put(DATA_JOB_INFO_ASPECT_NAME, new DataJobInfoTemplate());
    aspectSpecTemplateMap.put(
        DATA_PRODUCT_PROPERTIES_ASPECT_NAME, new DataProductPropertiesTemplate());
    aspectSpecTemplateMap.put(CHART_INFO_ASPECT_NAME, new ChartInfoTemplate());
    aspectSpecTemplateMap.put(DASHBOARD_INFO_ASPECT_NAME, new DashboardInfoTemplate());
    aspectSpecTemplateMap.put(DATA_JOB_INPUT_OUTPUT_ASPECT_NAME, new DataJobInputOutputTemplate());
    aspectSpecTemplateMap.put(
        STRUCTURED_PROPERTIES_ASPECT_NAME, new StructuredPropertiesTemplate());
    aspectSpecTemplateMap.put(
        STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME, new StructuredPropertyDefinitionTemplate());
    aspectSpecTemplateMap.put(FORM_INFO_ASPECT_NAME, new FormInfoTemplate());
    aspectSpecTemplateMap.put(VERSION_PROPERTIES_ASPECT_NAME, new VersionPropertiesTemplate());
    return new AspectTemplateEngine(aspectSpecTemplateMap);
  }

  @Nonnull
  @Override
  public EntitySpec getEntitySpec(@Nonnull final String entityName) {
    String lowercaseEntityName = entityName.toLowerCase();
    if (!entityNameToSpec.containsKey(lowercaseEntityName)) {
      throw new IllegalArgumentException(
          String.format("Failed to find entity with name %s in EntityRegistry", entityName));
    }
    return entityNameToSpec.get(lowercaseEntityName);
  }

  @Nonnull
  @Override
  public Map<String, EntitySpec> getEntitySpecs() {
    return entityNameToSpec;
  }

  @Nonnull
  @Override
  public Map<String, EventSpec> getEventSpecs() {
    return new HashMap<>();
  }

  @Nonnull
  @Override
  public AspectTemplateEngine getAspectTemplateEngine() {
    return _aspectTemplateEngine;
  }

  @Override
  public EventSpec getEventSpec(@Nonnull final String ignored) {
    return null;
  }

  public static SnapshotEntityRegistry getInstance() {
    return INSTANCE;
  }

  @Override
  @Nonnull
  public Map<String, AspectSpec> getAspectSpecs() {
    return _aspectNameToSpec;
  }
}
