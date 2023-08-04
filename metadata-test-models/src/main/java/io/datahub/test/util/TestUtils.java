package io.datahub.test.util;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TestUtils {
  /**
   * Returns the entity types that support Metadata Tests,
   * based on the presence of the required "testResults" aspect.
   *
   * @param entityRegistry the entity registry
   */
  public static Set<String> getSupportedEntityTypes(final EntityRegistry entityRegistry) {
    if (entityRegistry == null) {
      return Collections.emptySet();
    }
    return entityRegistry.getEntitySpecs().values().stream().filter(value -> value.hasAspect(Constants.TEST_RESULTS_ASPECT_NAME))
      .map(EntitySpec::getName)
      .collect(Collectors.toSet());
  }

  private TestUtils() {
  }

}
