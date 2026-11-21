package com.linkedin.metadata.config.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.testng.annotations.Test;

/** The shipped {@code application.yaml} bindings bind into the nested triggers/includes shape. */
public class SemanticProfilesBindingTest {

  @Test
  public void testShippedBindingsBindNested() throws Exception {
    StandardEnvironment environment = new StandardEnvironment();
    new YamlPropertySourceLoader()
        .load("application", new ClassPathResource("application.yaml"))
        .forEach(environment.getPropertySources()::addLast);

    SemanticProfilesConfiguration profiles =
        Binder.get(environment)
            .bind(
                "elasticsearch.entity-index.semantic-search.profiles",
                SemanticProfilesConfiguration.class)
            .get();

    SemanticBindingConfig documentText = profiles.getBindings().get("documentText");
    assertEquals(
        documentText.getTriggers(),
        Map.of("documentInfo", List.of("contents.text"), "semanticText", List.of("text")));
    assertTrue(documentText.getIncludes().isEmpty());
    assertTrue(profiles.getBindings().get("external").isExternal());
  }
}
