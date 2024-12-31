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
import org.apache.maven.artifact.versioning.ComparableVersion;

/**
 * Implementation of {@link EntityRegistry} that is similar to {@link ConfigEntityRegistry} but
 * different in one important way. It builds potentially partially specified {@link
 * com.linkedin.metadata.models.PartialEntitySpec} objects from an entity registry config yaml file
 */
@Slf4j
public class PatchEntityRegistry implements EntityRegistry {

  private final DataSchemaFactory dataSchemaFactory;
  @Getter private final PluginFactory pluginFactory;

  @Getter @Nullable
  private BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider;

  private final Map<String, EntitySpec> entityNameToSpec;
  private final Map<String, EventSpec> eventNameToSpec;
  private final Map<String, AspectSpec> _aspectNameToSpec;

  private final String registryName;
  private final ComparableVersion registryVersion;
  private final String identifier;

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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PatchEntityRegistry[" + "identifier=" + identifier + ';');
    entityNameToSpec.forEach(
        (key1, value1) ->
            sb.append("[entityName=")
                .append(key1)
                .append(";aspects=[")
                .append(
                    value1.getAspectSpecs().stream()
                        .map(AspectSpec::getName)
                        .collect(Collectors.joining(",")))
                .append("]]"));
    eventNameToSpec.forEach((key, value) -> sb.append("[eventName=").append(key).append("]"));
    return sb.toString();
  }

  public PatchEntityRegistry(
      Pair<Path, Path> configFileClassPathPair,
      String registryName,
      ComparableVersion registryVersion,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider)
      throws IOException, EntityRegistryException {
    this(
        DataSchemaFactory.withCustomClasspath(configFileClassPathPair.getSecond()),
        DataSchemaFactory.getClassLoader(configFileClassPathPair.getSecond())
            .map(Stream::of)
            .orElse(Stream.empty())
            .collect(Collectors.toList()),
        configFileClassPathPair.getFirst(),
        registryName,
        registryVersion,
        pluginFactoryProvider);
  }

  public PatchEntityRegistry(
      String entityRegistryRoot,
      String registryName,
      ComparableVersion registryVersion,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider)
      throws EntityRegistryException, IOException {
    this(
        getFileAndClassPath(entityRegistryRoot),
        registryName,
        registryVersion,
        pluginFactoryProvider);
  }

  private static Pair<Path, Path> getFileAndClassPath(String entityRegistryRoot)
      throws IOException, EntityRegistryException {
    Path entityRegistryRootLoc = Paths.get(entityRegistryRoot);
    if (Files.isDirectory(entityRegistryRootLoc)) {
      // Look for entity-registry.yml or entity-registry.yaml in the root folder
      List<Path> yamlFiles =
          Files.walk(entityRegistryRootLoc, 1)
              .filter(Files::isRegularFile)
              .filter(f -> f.endsWith("entity-registry.yml") || f.endsWith("entity-registry.yaml"))
              .collect(Collectors.toList());
      if (yamlFiles.isEmpty()) {
        throw new EntityRegistryException(
            String.format(
                "Did not find an entity registry (entity-registry.yaml/yml) under %s",
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
      log.info("Loading bare entity registry file at {}", entityRegistryRootLoc);
      return new Pair<>(entityRegistryRootLoc, null);
    }
  }

  public PatchEntityRegistry(
      DataSchemaFactory dataSchemaFactory,
      List<ClassLoader> classLoaders,
      Path configFilePath,
      String registryName,
      ComparableVersion registryVersion,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider)
      throws FileNotFoundException, EntityRegistryException {
    this(
        dataSchemaFactory,
        classLoaders,
        new FileInputStream(configFilePath.toString()),
        registryName,
        registryVersion,
        pluginFactoryProvider);
  }

  private PatchEntityRegistry(
      DataSchemaFactory dataSchemaFactory,
      List<ClassLoader> classLoaders,
      InputStream configFileStream,
      String registryName,
      ComparableVersion registryVersion,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider)
      throws EntityRegistryException {
    this.dataSchemaFactory = dataSchemaFactory;
    this.registryName = registryName;
    this.registryVersion = registryVersion;
    entityNameToSpec = new HashMap<>();
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
      log.error("Unable to read Patch configuration.", e);
      throw new IllegalArgumentException(
          String.format(
              "Error while reading config file in path %s: %s", configFileStream, e.getMessage()));
    }
    if (entities.getId() != null) {
      identifier = entities.getId();
    } else {
      identifier = "Unknown";
    }

    // Build Entity Specs
    EntitySpecBuilder entitySpecBuilder = new EntitySpecBuilder();
    for (Entity entity : entities.getEntities()) {
      log.info(
          "Discovered entity {} with aspects {}",
          entity.getName(),
          entity.getAspects().stream().collect(Collectors.joining()));
      List<AspectSpec> aspectSpecs = new ArrayList<>();
      if (entity.getKeyAspect() != null) {
        AspectSpec keyAspectSpec = buildAspectSpec(entity.getKeyAspect(), entitySpecBuilder);
        log.info("Adding key aspect {} with spec {}", entity.getKeyAspect(), keyAspectSpec);
        aspectSpecs.add(keyAspectSpec);
      }
      entity
          .getAspects()
          .forEach(
              aspect -> {
                if (!aspect.equals(entity.getKeyAspect())) {
                  AspectSpec aspectSpec = buildAspectSpec(aspect, entitySpecBuilder);
                  log.info("Adding aspect {} with spec {}", aspect, aspectSpec);
                  aspectSpecs.add(aspectSpec);
                }
              });

      EntitySpec entitySpec =
          entitySpecBuilder.buildPartialEntitySpec(
              entity.getName(), entity.getKeyAspect(), aspectSpecs);

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
    _aspectNameToSpec = populateAspectMap(new ArrayList<>(entityNameToSpec.values()));
  }

  @Override
  public String getIdentifier() {
    return this.identifier;
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
    String lowercaseEventName = eventName.toLowerCase();
    if (!eventNameToSpec.containsKey(lowercaseEventName)) {
      throw new IllegalArgumentException(
          String.format("Failed to find event with name %s in EntityRegistry", eventName));
    }
    return eventNameToSpec.get(lowercaseEventName);
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
    // TODO: support patch based templates

    return new AspectTemplateEngine();
  }

  private AspectSpec buildAspectSpec(String aspectName, EntitySpecBuilder entitySpecBuilder) {
    Optional<DataSchema> aspectSchema = dataSchemaFactory.getAspectSchema(aspectName);
    Optional<Class> aspectClass = dataSchemaFactory.getAspectClass(aspectName);
    if (!aspectSchema.isPresent()) {
      throw new IllegalArgumentException(String.format("Aspect %s does not exist", aspectName));
    }
    AspectSpec aspectSpec =
        entitySpecBuilder.buildAspectSpec(aspectSchema.get(), aspectClass.get());
    aspectSpec.setRegistryName(this.registryName);
    aspectSpec.setRegistryVersion(this.registryVersion);
    return aspectSpec;
  }

  private EventSpec buildEventSpec(String eventName) {
    Optional<DataSchema> eventSchema = dataSchemaFactory.getEventSchema(eventName);
    if (!eventSchema.isPresent()) {
      throw new IllegalArgumentException(String.format("Event %s does not exist", eventName));
    }
    return new EventSpecBuilder().buildEventSpec(eventName, eventSchema.get());
  }
}
