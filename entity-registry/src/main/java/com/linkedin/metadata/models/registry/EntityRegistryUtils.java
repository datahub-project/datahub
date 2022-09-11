package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class EntityRegistryUtils {
  private EntityRegistryUtils() {

  }

  public static Map<String, AspectSpec> populateAspectMap(List<EntitySpec> entitySpecs) {
    return entitySpecs.stream()
        .map(EntitySpec::getAspectSpecs)
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(AspectSpec::getName, Function.identity(), (aspectSpec1, aspectSpec2) -> aspectSpec1));
  }

}
