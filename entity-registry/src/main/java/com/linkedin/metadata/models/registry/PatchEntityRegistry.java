package com.linkedin.metadata.models.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.data.schema.DataSchema;
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
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.artifact.versioning.ComparableVersion;


/**
 * Implementation of {@link EntityRegistry} that is similar to {@link ConfigEntityRegistry} but different in one important way.
 * It builds potentially partially specified {@link com.linkedin.metadata.models.PartialEntitySpec} objects from an entity registry config yaml file
 */
@Slf4j
public class PatchEntityRegistry implements EntityRegistry {

  private final DataSchemaFactory dataSchemaFactory;
  private final Map<String, EntitySpec> entityNameToSpec;
  private final Map<String, EventSpec> eventNameToSpec;

  private final String registryName;
  private final ComparableVersion registryVersion;
  private final String identifier;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PatchEntityRegistry[" + "identifier=" + identifier + ';');
    entityNameToSpec.entrySet()
        .stream()
        .forEach(entry -> sb.append("[entityName=")
            .append(entry.getKey())
            .append(";aspects=[")
            .append(
                entry.getValue().getAspectSpecs().stream().map(spec -> spec.getName()).collect(Collectors.joining(",")))
            .append("]]"));
    eventNameToSpec.entrySet()
        .stream()
        .forEach(entry -> sb.append("[eventName=")
            .append(entry.getKey())
            .append("]"));
    return sb.toString();
  }

  public PatchEntityRegistry(Pair<Path, Path> configFileClassPathPair, String registryName,
      ComparableVersion registryVersion) throws IOException, EntityRegistryException {
    this(DataSchemaFactory.withCustomClasspath(configFileClassPathPair.getSecond()), configFileClassPathPair.getFirst(),
        registryName, registryVersion);
  }

  public PatchEntityRegistry(String entityRegistryRoot, String registryName, ComparableVersion registryVersion)
      throws EntityRegistryException, IOException {
    this(getFileAndClassPath(entityRegistryRoot), registryName, registryVersion);
  }

  private static Pair<Path, Path> getFileAndClassPath(String entityRegistryRoot)
      throws IOException, EntityRegistryException {
    Path entityRegistryRootLoc = Paths.get(entityRegistryRoot);
    if (Files.isDirectory(entityRegistryRootLoc)) {
      // Look for entity-registry.yml or entity-registry.yaml in the root folder
      List<Path> yamlFiles = Files.walk(entityRegistryRootLoc, 1)
          .filter(Files::isRegularFile)
          .filter(f -> f.endsWith("entity-registry.yml") || f.endsWith("entity-registry.yaml"))
          .collect(Collectors.toList());
      if (yamlFiles.size() == 0) {
        throw new EntityRegistryException(
            String.format("Did not find an entity registry (entity-registry.yaml/yml) under %s",
                entityRegistryRootLoc));
      }
      if (yamlFiles.size() > 1) {
        log.warn("Found more than one yaml file in the directory {}. Will pick the first {}", entityRegistryRootLoc,
            yamlFiles.get(0));
      }
      Path entityRegistryFile = yamlFiles.get(0);
      log.info("Loading custom config entity file: {}, dir: {}", entityRegistryFile, entityRegistryRootLoc);
      return new Pair<>(entityRegistryFile, entityRegistryRootLoc);
    } else {
      // We assume that the file being passed in is a bare entity registry yaml file
      log.info("Loading bare entity registry file at {}", entityRegistryRootLoc);
      return new Pair<>(entityRegistryRootLoc, null);
    }
  }

  public PatchEntityRegistry(DataSchemaFactory dataSchemaFactory, Path configFilePath, String registryName,
      ComparableVersion registryVersion) throws FileNotFoundException, EntityRegistryException {
    this(dataSchemaFactory, new FileInputStream(configFilePath.toString()), registryName, registryVersion);
  }

  private PatchEntityRegistry(DataSchemaFactory dataSchemaFactory, InputStream configFileStream, String registryName,
      ComparableVersion registryVersion) throws EntityRegistryException {
    this.dataSchemaFactory = dataSchemaFactory;
    this.registryName = registryName;
    this.registryVersion = registryVersion;
    entityNameToSpec = new HashMap<>();
    Entities entities;
    try {
      entities = OBJECT_MAPPER.readValue(configFileStream, Entities.class);
    } catch (IOException e) {
      e.printStackTrace();
      throw new IllegalArgumentException(
          String.format("Error while reading config file in path %s: %s", configFileStream, e.getMessage()));
    }
    if (entities.getId() != null) {
      identifier = entities.getId();
    } else {
      identifier = "Unknown";
    }

    // Build Entity Specs
    EntitySpecBuilder entitySpecBuilder = new EntitySpecBuilder();
    for (Entity entity : entities.getEntities()) {
      log.info("Discovered entity {} with aspects {}", entity.getName(),
          entity.getAspects().stream().collect(Collectors.joining()));
      List<AspectSpec> aspectSpecs = new ArrayList<>();
      if (entity.getKeyAspect() != null) {
        throw new EntityRegistryException(
            "Patch Entities cannot define entities yet. They can only enhance an existing entity with additional (non-key) aspects");
        // aspectSpecs.add(getAspectSpec(entity.getKeyAspect(), entitySpecBuilder));
      }
      entity.getAspects().forEach(aspect -> {
        AspectSpec aspectSpec = buildAspectSpec(aspect, entitySpecBuilder);
        log.info("Adding aspect {} with spec {}", aspect, aspectSpec);
        aspectSpecs.add(aspectSpec);
      });

      EntitySpec entitySpec =
          entitySpecBuilder.buildPartialEntitySpec(entity.getName(), entity.getKeyAspect(), aspectSpecs);

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
  public Map<String, EventSpec> getEventSpecs() {
    return eventNameToSpec;
  }

  private AspectSpec buildAspectSpec(String aspectName, EntitySpecBuilder entitySpecBuilder) {
    Optional<DataSchema> aspectSchema = dataSchemaFactory.getAspectSchema(aspectName);
    Optional<Class> aspectClass = dataSchemaFactory.getAspectClass(aspectName);
    if (!aspectSchema.isPresent()) {
      throw new IllegalArgumentException(String.format("Aspect %s does not exist", aspectName));
    }
    AspectSpec aspectSpec = entitySpecBuilder.buildAspectSpec(aspectSchema.get(), aspectClass.get());
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
