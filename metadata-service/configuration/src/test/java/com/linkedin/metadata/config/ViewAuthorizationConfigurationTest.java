package com.linkedin.metadata.config;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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

    String defaultValue =
        environment.getRequiredProperty("authorization.view.unrestrictedEntityTypes.defaultValue");
    assertFalse(
        Arrays.stream(defaultValue.split(","))
            .map(String::trim)
            .anyMatch(entityType::equalsIgnoreCase),
        entityType + " must not be in the stock unrestricted defaults");
    assertTrue(
        environment
            .resolvePlaceholders(
                environment.getRequiredProperty("authorization.view.unrestrictedEntityTypes.add"))
            .isEmpty(),
        "VIEW_UNRESTRICTED_ENTITY_TYPES_ADD must default to empty");
  }
}
