package com.linkedin.metadata.models.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.*;
import com.linkedin.metadata.models.registry.config.Entities;
import com.linkedin.util.Pair;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
public class EntityRegistryUtils {
  private EntityRegistryUtils() {

  }

  public static Entities readEntities(final ObjectMapper obj_mapper, InputStream configFileStream) {
    Entities entities;
    try {
      entities = obj_mapper.readValue(configFileStream, Entities.class);
    } catch (IOException e) {
      log.error("Error during reading entity registry", e);
      throw new IllegalArgumentException(
              String.format("Error while reading config file in path %s: %s", configFileStream, e.getMessage()));
    }
    return entities;
  }

  public static Map<String, AspectSpec> populateAspectMap(List<EntitySpec> entitySpecs) {
    return entitySpecs.stream()
        .map(EntitySpec::getAspectSpecs)
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(AspectSpec::getName, Function.identity(), (aspectSpec1, aspectSpec2) -> aspectSpec1));
  }
  
  public static EventSpec buildEventSpec(String eventName, final DataSchemaFactory dataSchemaFactory) {
    Optional<DataSchema> eventSchema = dataSchemaFactory.getEventSchema(eventName);
    if (eventSchema.isEmpty()) {
      throw new IllegalArgumentException(String.format("Event %s does not exist", eventName));
    }
    return new EventSpecBuilder().buildEventSpec(eventName, eventSchema.get());
  }

  public static Pair<Path, Path> getFileAndClassPath(String entityRegistryRoot)
          throws IOException, EntityRegistryException {
    Path entityRegistryRootLoc = Paths.get(entityRegistryRoot);
    if (Files.isDirectory(entityRegistryRootLoc)) {
      List<Path> yamlFiles = getRegistryFiles(entityRegistryRootLoc);
      if (yamlFiles.size() > 1) {
        log.warn("Found more than one yaml file in the directory {}. Will pick the first {}",
                entityRegistryRootLoc, yamlFiles.get(0));
      }
      Path entityRegistryFile = yamlFiles.get(0);
      log.info("Loading config entity file: {}, dir: {}", entityRegistryFile, entityRegistryRootLoc);
      return new Pair<>(entityRegistryFile, entityRegistryRootLoc);
    } else {
      // We assume that the file being passed in is a bare entity registry yaml file
      log.info("Loading bare config entity registry file at {}", entityRegistryRootLoc);
      return new Pair<>(entityRegistryRootLoc, null);
    }
  }

  public static List<Path> getRegistryFiles(Path entityRegistryRootLoc) throws IOException, EntityRegistryException {
    List<Path> yamlFiles = Files.walk(entityRegistryRootLoc, 1)
            .filter(Files::isRegularFile)
            .filter(f -> f.toString().endsWith("entity-registry.yml") || f.toString().endsWith("entity-registry.yaml"))
            .collect(Collectors.toList());
    if (yamlFiles.size() == 0) {
      throw new EntityRegistryException(
              String.format("Did not find an entity registry (entity-registry.yaml/yml) under %s", entityRegistryRootLoc));
    }
    return yamlFiles;
  }

}
