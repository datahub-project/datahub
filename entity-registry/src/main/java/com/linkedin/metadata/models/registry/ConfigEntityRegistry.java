package com.linkedin.metadata.models.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.registry.config.Entities;
import com.linkedin.metadata.models.registry.config.Entity;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;


/**
 * Implementation of {@link EntityRegistry} that builds {@link DefaultEntitySpec} objects
 * from an entity registry config yaml file
 */
public class ConfigEntityRegistry implements EntityRegistry {

  private final DataSchemaFactory dataSchemaFactory;
  private final Map<String, EntitySpec> entityNameToSpec;
  private final List<EntitySpec> entitySpecs;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public ConfigEntityRegistry(String configFilePath) throws FileNotFoundException {
    this(DataSchemaFactory.getInstance(), new FileInputStream(configFilePath));
  }

  public ConfigEntityRegistry(InputStream configFileInputStream) {
    this(DataSchemaFactory.getInstance(), configFileInputStream);
  }

  public ConfigEntityRegistry(DataSchemaFactory dataSchemaFactory, String configFilePath) throws FileNotFoundException {
    this(dataSchemaFactory, new FileInputStream(configFilePath));
  }

  public ConfigEntityRegistry(DataSchemaFactory dataSchemaFactory, InputStream configFileStream) {
    this.dataSchemaFactory = dataSchemaFactory;
    entityNameToSpec = new HashMap<>();
    Entities entities;
    try {
      entities = OBJECT_MAPPER.readValue(configFileStream, Entities.class);
    } catch (IOException e) {
      e.printStackTrace();
      throw new IllegalArgumentException(
          String.format("Error while reading config file in path %s: %s", configFileStream, e.getMessage()));
    }
    EntitySpecBuilder entitySpecBuilder = new EntitySpecBuilder();
    for (Entity entity : entities.getEntities()) {
      Optional<DataSchema> entitySchema = dataSchemaFactory.getEntitySchema(entity.getName());
      if (!entitySchema.isPresent()) {
        throw new IllegalArgumentException(String.format("Entity %s does not exist", entity.getName()));
      }

      List<AspectSpec> aspectSpecs = new ArrayList<>();
      aspectSpecs.add(getAspectSpec(entity.getKeyAspect(), entitySpecBuilder));
      entity.getAspects().forEach(aspect -> aspectSpecs.add(getAspectSpec(aspect, entitySpecBuilder)));

      EntitySpec entitySpec = entitySpecBuilder.buildEntitySpec(entitySchema.get(), aspectSpecs);

      entityNameToSpec.put(entity.getName().toLowerCase(), entitySpec);
    }
    entitySpecs = new ArrayList<>(entityNameToSpec.values());
  }

  private AspectSpec getAspectSpec(String aspectName, EntitySpecBuilder entitySpecBuilder) {
    Optional<DataSchema> aspectSchema = dataSchemaFactory.getAspectSchema(aspectName);
    if (!aspectSchema.isPresent()) {
      throw new IllegalArgumentException(String.format("Aspect %s does not exist", aspectName));
    }
    return entitySpecBuilder.buildAspectSpec(aspectSchema.get());
  }

  @Nonnull
  @Override
  public EntitySpec getEntitySpec(@Nonnull String entityName) {
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
}
