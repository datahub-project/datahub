package com.linkedin.metadata.models.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.*;
import com.linkedin.metadata.models.registry.config.Entities;
import com.linkedin.metadata.models.registry.config.Entity;
import com.linkedin.metadata.models.registry.config.Event;
import com.linkedin.util.Pair;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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

  public static List<InputStream> getRegistryFiles(String entityRegistryRoot) throws EntityRegistryException, IOException {
    return getRegistryFiles(Paths.get(entityRegistryRoot))
            .stream()
            .map(path -> {
              try {
                return new FileInputStream(path.toString());
              } catch (FileNotFoundException e) {
                log.error("Unable to read registry file at path {}", path);
              }
              return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
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

  public static String getIdentifier(Entities entities) {
    if (entities.getId() != null) {
      return entities.getId();
    } else {
      return  "Unknown";
    }
  }

  public static Map<String, EventSpec> getEventNameToSpec(Entities entities, DataSchemaFactory dataSchemaFactory) {
    Map<String, EventSpec> result = new HashMap<>();
    if (entities.getEvents() != null) {
      for (Event event : entities.getEvents()) {
        EventSpec eventSpec = EntityRegistryUtils.buildEventSpec(event.getName(), dataSchemaFactory);
        result.put(event.getName().toLowerCase(), eventSpec);
      }
    }
    return result;
  }

  public static Entities mergeEntities(List<Entities> entities) {
    if (entities.size() <= 0) {
      throw new RuntimeException("No entities present");
    }
    if (entities.size() == 1) {
      return entities.get(0);
    }
    String id = entities.get(0).getId();
    List<Entity> finalEntityMap = new ArrayList<>();
    List<Event> finalEventMap = new ArrayList<>();

    groupEntitiesByIdentifier(entities).values().forEach(entityList -> {
      finalEntityMap.add(mergeEntity(entityList));
    });
    Map<String, List<Event>> eventByName = groupEventsByIdentifier(entities);
    //TODO Do the actual merge for events

    return new Entities(id, finalEntityMap, finalEventMap);
  }

  public static Map<String, List<Entity>> groupEntitiesByIdentifier(List<Entities> entitiesList) {
    Map<String, List<Entity>> resultMap = new HashMap<>();
    for (Entities entities : entitiesList) {
      for (Entity entity : entities.getEntities()) {
        String identifier = entity.getName();
        List<Entity> entitiesWithSameIdentifier = resultMap.getOrDefault(identifier, new ArrayList<>());
        entitiesWithSameIdentifier.add(entity);
        resultMap.put(identifier, entitiesWithSameIdentifier);
      }
    }
    return resultMap;
  }

  public static Map<String, List<Event>> groupEventsByIdentifier(List<Entities> entitiesList) {
    Map<String, List<Event>> resultMap = new HashMap<>();
    for (Entities entities : entitiesList) {
      for (Event event : entities.getEvents()) {
        String identifier = event.getName();
        List<Event> entitiesWithSameIdentifier = resultMap.getOrDefault(identifier, new ArrayList<>());
        entitiesWithSameIdentifier.add(event);
        resultMap.put(identifier, entitiesWithSameIdentifier);
      }
    }
    return resultMap;
  }

  public static Entity mergeEntity(List<Entity> entities) {
    if (entities.size() <= 0) {
      return null;
    }
    if (entities.size() == 1) {
      return entities.get(0);
    }
    String finalName = null;
    String finalDoc = null;
    String finalKeyAspect = null;
    Set<String> finalAspects = new LinkedHashSet<>();
    for (Entity entity: entities) {
      finalName = entity.getName();
      finalDoc = entity.getDoc();
      finalKeyAspect = entity.getKeyAspect();
      for (String aspect: entity.getAspects()) {
        if (finalAspects.contains(aspect)) {
          throw new RuntimeException("Found duplicate aspect " + aspect + " for entity " + finalName);
        } else {
          finalAspects.add(aspect);
        }
      }
    }
    return new Entity(finalName, finalDoc, finalKeyAspect, new ArrayList<>(finalAspects));
  }

}
