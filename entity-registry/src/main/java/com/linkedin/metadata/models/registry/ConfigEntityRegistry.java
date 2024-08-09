package com.linkedin.metadata.models.registry;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.models.registry.EntityRegistryUtils.*;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.EventSpecBuilder;
import com.linkedin.metadata.models.registry.config.Entities;
import com.linkedin.metadata.models.registry.config.Entity;
import com.linkedin.metadata.models.registry.config.Event;
import com.linkedin.util.Pair;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link EntityRegistry} that builds {@link DefaultEntitySpec} objects from an
 * entity registry config yaml file
 */
@Slf4j
public class ConfigEntityRegistry implements EntityRegistry {

  private final DataSchemaFactory dataSchemaFactory;
  @Getter private final PluginFactory pluginFactory;

  @Getter @Nullable
  private BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider;

  private final Map<String, EntitySpec> entityNameToSpec;
  private final Map<String, EventSpec> eventNameToSpec;
  private final List<EntitySpec> entitySpecs;
  private final String identifier;
  private final Map<String, AspectSpec> _aspectNameToSpec;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  public ConfigEntityRegistry(
      Pair<Path, Path> configFileClassPathPair,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider)
      throws IOException {
    this(
        DataSchemaFactory.withCustomClasspath(configFileClassPathPair.getSecond()),
        DataSchemaFactory.getClassLoader(configFileClassPathPair.getSecond())
            .map(Stream::of)
            .orElse(Stream.empty())
            .collect(Collectors.toList()),
        configFileClassPathPair.getFirst(),
        pluginFactoryProvider);
  }

  public ConfigEntityRegistry(
      String entityRegistryRoot,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider)
      throws EntityRegistryException, IOException {
    this(getFileAndClassPath(entityRegistryRoot), pluginFactoryProvider);
  }

  private static Pair<Path, Path> getFileAndClassPath(String entityRegistryRoot)
      throws IOException, EntityRegistryException {
    Path entityRegistryRootLoc = Paths.get(entityRegistryRoot);
    if (Files.isDirectory(entityRegistryRootLoc)) {
      // Look for entity_registry.yml or entity_registry.yaml in the root folder
      List<Path> yamlFiles =
          Files.walk(entityRegistryRootLoc, 1)
              .filter(Files::isRegularFile)
              .filter(f -> f.endsWith("entity-registry.yml") || f.endsWith("entity-registry.yaml"))
              .collect(Collectors.toList());
      if (yamlFiles.isEmpty()) {
        throw new EntityRegistryException(
            String.format(
                "Did not find an entity registry (entity_registry.yaml/yml) under %s",
                entityRegistryRootLoc));
      }
      if (yamlFiles.size() > 1) {
        log.warn(
            "Found more than one yaml file in the directory {}. Will pick the first {}",
            entityRegistryRootLoc,
            yamlFiles.get(0));
      }
      Path entityRegistryFile = yamlFiles.get(0);
      log.info(
          "Loading custom config entity file: {}, dir: {}",
          entityRegistryFile,
          entityRegistryRootLoc);
      return new Pair<>(entityRegistryFile, entityRegistryRootLoc);
    } else {
      // We assume that the file being passed in is a bare entity registry yaml file
      log.info("Loading bare config entity registry file at {}", entityRegistryRootLoc);
      return new Pair<>(entityRegistryRootLoc, null);
    }
  }

  public ConfigEntityRegistry(InputStream configFileInputStream) {
    this(configFileInputStream, null);
  }

  public ConfigEntityRegistry(
      InputStream configFileInputStream,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider) {
    this(
        DataSchemaFactory.getInstance(),
        Collections.emptyList(),
        configFileInputStream,
        pluginFactoryProvider);
  }

  public ConfigEntityRegistry(
      DataSchemaFactory dataSchemaFactory,
      List<ClassLoader> classLoaders,
      Path configFilePath,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider)
      throws FileNotFoundException {
    this(
        dataSchemaFactory,
        classLoaders,
        new FileInputStream(configFilePath.toString()),
        pluginFactoryProvider);
  }

  public ConfigEntityRegistry(
      DataSchemaFactory dataSchemaFactory,
      List<ClassLoader> classLoaders,
      InputStream configFileStream) {
    this(dataSchemaFactory, classLoaders, configFileStream, null);
  }

  public ConfigEntityRegistry(
      DataSchemaFactory dataSchemaFactory,
      List<ClassLoader> classLoaders,
      InputStream configFileStream,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider) {
    this.dataSchemaFactory = dataSchemaFactory;
    Entities entities;
    try {
      entities = OBJECT_MAPPER.readValue(configFileStream, Entities.class);
      if (pluginFactoryProvider != null) {
        this.pluginFactory = pluginFactoryProvider.apply(entities.getPlugins(), classLoaders);
      } else {
        this.pluginFactory = PluginFactory.withCustomClasspath(entities.getPlugins(), classLoaders);
      }
      this.pluginFactoryProvider = pluginFactoryProvider;
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format(
              "Error while reading config file in path %s: %s", configFileStream, e.getMessage()),
          e);
    }
    if (entities.getId() != null) {
      identifier = entities.getId();
    } else {
      identifier = "Unknown";
    }

    // Build Entity Specs
    entityNameToSpec = new HashMap<>();
    EntitySpecBuilder entitySpecBuilder = new EntitySpecBuilder();
    for (Entity entity : entities.getEntities()) {
      List<AspectSpec> aspectSpecs = new ArrayList<>();
      aspectSpecs.add(buildAspectSpec(entity.getKeyAspect(), entitySpecBuilder));
      entity
          .getAspects()
          .forEach(aspect -> aspectSpecs.add(buildAspectSpec(aspect, entitySpecBuilder)));

      EntitySpec entitySpec;
      Optional<DataSchema> entitySchema = dataSchemaFactory.getEntitySchema(entity.getName());
      if (!entitySchema.isPresent()) {
        entitySpec =
            entitySpecBuilder.buildConfigEntitySpec(
                entity.getName(), entity.getKeyAspect(), aspectSpecs);
      } else {
        entitySpec = entitySpecBuilder.buildEntitySpec(entitySchema.get(), aspectSpecs);
      }
      entityNameToSpec.put(entity.getName().toLowerCase(), entitySpec);
    }

    // Build Event Specs
    eventNameToSpec = new HashMap<>();
    if (entities.getEvents() != null) {
      for (Event event : entities.getEvents()) {
        EventSpec eventSpec = buildEventSpec(event.getName());
        eventNameToSpec.put(event.getName().toLowerCase(), eventSpec);
      }
    }
    entitySpecs = new ArrayList<>(entityNameToSpec.values());
    _aspectNameToSpec = populateAspectMap(entitySpecs);
  }

  @Override
  public String getIdentifier() {
    return this.identifier;
  }

  private AspectSpec buildAspectSpec(String aspectName, EntitySpecBuilder entitySpecBuilder) {
    Optional<DataSchema> aspectSchema = dataSchemaFactory.getAspectSchema(aspectName);
    Optional<Class> aspectClass = dataSchemaFactory.getAspectClass(aspectName);
    if (!aspectSchema.isPresent()) {
      throw new IllegalArgumentException(String.format("Aspect %s does not exist", aspectName));
    }
    return entitySpecBuilder.buildAspectSpec(aspectSchema.get(), aspectClass.get());
  }

  private EventSpec buildEventSpec(String eventName) {
    Optional<DataSchema> eventSchema = dataSchemaFactory.getEventSchema(eventName);
    if (!eventSchema.isPresent()) {
      throw new IllegalArgumentException(String.format("Event %s does not exist", eventName));
    }
    return new EventSpecBuilder().buildEventSpec(eventName, eventSchema.get());
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
  public EventSpec getEventSpec(@Nonnull String eventName) {
    String lowerEventName = eventName.toLowerCase();
    if (!eventNameToSpec.containsKey(lowerEventName)) {
      throw new IllegalArgumentException(
          String.format("Failed to find event with name %s in EntityRegistry", eventName));
    }
    return eventNameToSpec.get(lowerEventName);
  }

  @Nonnull
  @Override
  public Map<String, EntitySpec> getEntitySpecs() {
    return entityNameToSpec;
  }

  @Nonnull
  @Override
  public Map<String, AspectSpec> getAspectSpecs() {
    return _aspectNameToSpec;
  }

  @Nonnull
  @Override
  public Map<String, EventSpec> getEventSpecs() {
    return eventNameToSpec;
  }

  @Nonnull
  @Override
  public AspectTemplateEngine getAspectTemplateEngine() {

    // TODO: add support for config based aspect templates
    return new AspectTemplateEngine();
  }
}
