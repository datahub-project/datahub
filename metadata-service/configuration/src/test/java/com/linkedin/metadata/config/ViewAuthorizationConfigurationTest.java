package com.linkedin.metadata.config;

import static org.testng.Assert.assertFalse;

import java.util.Arrays;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.testng.annotations.Test;

public class ViewAuthorizationConfigurationTest {

  @Test
  public void testDocumentIsNotUnrestrictedByDefault() throws Exception {
    assertNotInStockAdd("document");
  }

  @Test
  public void testSchemaFieldIsNotUnrestrictedByDefault() throws Exception {
    assertNotInStockAdd("schemaField");
  }

  @Test
  public void testContainerIsNotUnrestrictedByDefault() throws Exception {
    assertNotInStockAdd("container");
  }

  private static void assertNotInStockAdd(String entityType) throws Exception {
    StandardEnvironment environment = new StandardEnvironment();
    new YamlPropertySourceLoader()
        .load("application", new ClassPathResource("application.yaml"))
        .forEach(environment.getPropertySources()::addLast);

    String defaultAdd =
        environment.getRequiredProperty("authorization.view.unrestrictedEntityTypes.add");
    assertFalse(
        Arrays.stream(defaultAdd.split(","))
            .map(String::trim)
            .anyMatch(entityType::equalsIgnoreCase),
        entityType + " must not be in stock VIEW_UNRESTRICTED_ENTITY_TYPES_ADD");
  }
}
