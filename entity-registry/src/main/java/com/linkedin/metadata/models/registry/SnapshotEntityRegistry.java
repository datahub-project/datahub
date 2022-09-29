package com.linkedin.metadata.models.registry;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.registry.template.AspectTemplateEngine;
import com.linkedin.metadata.models.registry.template.Template;
import com.linkedin.metadata.models.registry.template.common.GlobalTagsTemplate;
import com.linkedin.metadata.models.registry.template.common.GlossaryTermsTemplate;
import com.linkedin.metadata.models.registry.template.common.OwnershipTemplate;
import com.linkedin.metadata.models.registry.template.dataset.DatasetPropertiesTemplate;
import com.linkedin.metadata.models.registry.template.dataset.EditableSchemaMetadataTemplate;
import com.linkedin.metadata.models.registry.template.dataset.UpstreamLineageTemplate;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.models.registry.EntityRegistryUtils.*;


/**
 * Implementation of {@link EntityRegistry} that builds {@link DefaultEntitySpec} objects
 * from the a {@link Snapshot} Record Template present on the classpath
 */
public class SnapshotEntityRegistry implements EntityRegistry {

  private final Map<String, EntitySpec> entityNameToSpec;
  private final List<EntitySpec> entitySpecs;
  private final AspectTemplateEngine _aspectTemplateEngine;
  private final Map<String, AspectSpec> _aspectNameToSpec;

  private static final SnapshotEntityRegistry INSTANCE = new SnapshotEntityRegistry();

  public SnapshotEntityRegistry() {
    entityNameToSpec = new EntitySpecBuilder().buildEntitySpecs(new Snapshot().schema())
        .stream()
        .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
    entitySpecs = new ArrayList<>(entityNameToSpec.values());
    _aspectNameToSpec = populateAspectMap(entitySpecs);
    _aspectTemplateEngine = populateTemplateEngine(_aspectNameToSpec);
  }

  public SnapshotEntityRegistry(UnionTemplate snapshot) {
    entityNameToSpec = new EntitySpecBuilder().buildEntitySpecs(snapshot.schema())
        .stream()
        .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
    entitySpecs = new ArrayList<>(entityNameToSpec.values());
    _aspectNameToSpec = populateAspectMap(entitySpecs);
    _aspectTemplateEngine = populateTemplateEngine(_aspectNameToSpec);
  }

  private AspectTemplateEngine populateTemplateEngine(Map<String, AspectSpec> aspectSpecs) {
    // TODO: This should be more dynamic ideally, "hardcoding" for now, passing in aspect spec map preemptively

    Map<String, Template<? extends RecordTemplate>> aspectSpecTemplateMap = new HashMap<>();
    aspectSpecTemplateMap.put(OWNERSHIP_ASPECT_NAME, new OwnershipTemplate());
    aspectSpecTemplateMap.put(DATASET_PROPERTIES_ASPECT_NAME, new DatasetPropertiesTemplate());
    aspectSpecTemplateMap.put(UPSTREAM_LINEAGE_ASPECT_NAME, new UpstreamLineageTemplate());
    aspectSpecTemplateMap.put(GLOBAL_TAGS_ASPECT_NAME, new GlobalTagsTemplate());
    aspectSpecTemplateMap.put(EDITABLE_SCHEMA_METADATA_ASPECT_NAME, new EditableSchemaMetadataTemplate());
    aspectSpecTemplateMap.put(GLOSSARY_TERMS_ASPECT_NAME, new GlossaryTermsTemplate());
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
  public EventSpec getEventSpec(final String ignored) {
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
