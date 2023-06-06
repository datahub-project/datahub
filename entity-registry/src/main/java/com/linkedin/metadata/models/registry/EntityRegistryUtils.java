package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.util.Pair;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
public class EntityRegistryUtils {
  private EntityRegistryUtils() {

  }

  public static Map<String, AspectSpec> populateAspectMap(List<EntitySpec> entitySpecs) {
    return entitySpecs.stream()
        .map(EntitySpec::getAspectSpecs)
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(AspectSpec::getName, Function.identity(), (aspectSpec1, aspectSpec2) -> aspectSpec1));
  }

  public static Pair<Path, Path> getFileAndClassPath(String entityRegistryRoot)
          throws IOException, EntityRegistryException {
    Path entityRegistryRootLoc = Paths.get(entityRegistryRoot);
    if (Files.isDirectory(entityRegistryRootLoc)) {
      // Look for entity-registry.yml or entity-registry.yaml in the root folder
      List<Path> yamlFiles = Files.walk(entityRegistryRootLoc, 1)
              .filter(Files::isRegularFile)
              .filter(f -> f.toString().endsWith("entity-registry.yml") || f.toString().endsWith("entity-registry.yaml"))
              .collect(Collectors.toList());
      if (yamlFiles.size() == 0) {
        throw new EntityRegistryException(
                String.format("Did not find an entity registry (entity-registry.yaml/yml) under %s", entityRegistryRootLoc));
      }
      if (yamlFiles.size() > 1) {
        log.warn("Found more than one yaml file in the directory {}. Will pick the first {}",
                entityRegistryRootLoc, yamlFiles.get(0));
      }
      Path entityRegistryFile = yamlFiles.get(0);
      log.info("Loading custom config entity file: {}, dir: {}", entityRegistryFile, entityRegistryRootLoc);
      return new Pair<>(entityRegistryFile, entityRegistryRootLoc);
    } else {
      // We assume that the file being passed in is a bare entity registry yaml file
      log.info("Loading bare config entity registry file at {}", entityRegistryRootLoc);
      return new Pair<>(entityRegistryRootLoc, null);
    }
  }

}
