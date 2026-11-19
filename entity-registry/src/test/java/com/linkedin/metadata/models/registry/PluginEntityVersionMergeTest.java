package com.linkedin.metadata.models.registry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.PartialEntitySpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.testng.annotations.Test;

/** A newer version of a plugin that defines its own entity is merged onto the older version. */
public class PluginEntityVersionMergeTest {

  private static final String KEY_V1 =
      "{\"type\":\"record\",\"name\":\"ThingKey\",\"namespace\":\"test\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
  private static final String KEY_V2 =
      "{\"type\":\"record\",\"name\":\"ThingKey\",\"namespace\":\"test\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"},"
          + "{\"name\":\"region\",\"type\":\"string\",\"optional\":true}]}";
  private static final String INFO_V1 =
      "{\"type\":\"record\",\"name\":\"ThingInfo\",\"namespace\":\"test\","
          + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
  private static final String INFO_V2 =
      "{\"type\":\"record\",\"name\":\"ThingInfo\",\"namespace\":\"test\","
          + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},"
          + "{\"name\":\"note\",\"type\":\"string\",\"optional\":true}]}";
  private static final String INFO_INCOMPATIBLE =
      "{\"type\":\"record\",\"name\":\"ThingInfo\",\"namespace\":\"test\","
          + "\"fields\":[{\"name\":\"name\",\"type\":\"int\"}]}";
  private static final String EXTRA =
      "{\"type\":\"record\",\"name\":\"ThingExtra\",\"namespace\":\"test\","
          + "\"fields\":[{\"name\":\"x\",\"type\":\"string\",\"optional\":true}]}";

  @Test
  public void testSecondVersionOfPluginDefinedEntityLoads()
      throws Exception, EntityRegistryException {
    Path pluginLocation =
        Paths.get(
            TestConstants.BASE_DIRECTORY
                + "/"
                + TestConstants.TEST_REGISTRY
                + "/"
                + TestConstants.TEST_VERSION);
    Path registryYaml =
        Paths.get("src/test_plugins/mycompany-full-model/0.0.1/entity-registry.yaml");

    MergedEntityRegistry merged = new MergedEntityRegistry(registryOf());
    for (String version : List.of("0.0.1", "0.0.2")) {
      merged.apply(
          new PatchEntityRegistry(
              DataSchemaFactory.withCustomClasspath(pluginLocation),
              DataSchemaFactory.getClassLoader(pluginLocation).stream().toList(),
              registryYaml,
              "mycompany-full-model",
              new ComparableVersion(version),
              null));
    }

    EntitySpec newThing = merged.getEntitySpec("newThing");
    assertEquals(newThing.getKeyAspectName(), TestConstants.TEST_ASPECT_NAME);
    assertNotNull(newThing.getKeyAspectSpec());
  }

  @Test
  public void testNewerVersionReplacesAspectsAndKeepsOlderOnes()
      throws Exception, EntityRegistryException {
    MergedEntityRegistry merged =
        new MergedEntityRegistry(registryOf())
            .apply(registryOf(thing(aspect("thingKey", KEY_V1), aspect("thingInfo", INFO_V1))))
            .apply(registryOf(thing(aspect("thingKey", KEY_V1), aspect("thingExtra", EXTRA))))
            .apply(registryOf(thing(aspect("thingKey", KEY_V1), aspect("thingInfo", INFO_V2))));

    EntitySpec spec = merged.getEntitySpec("thing");
    assertTrue(spec instanceof PartialEntitySpec);
    assertEquals(spec.getKeyAspectName(), "thingKey");
    assertTrue(spec.hasAspect("thingExtra"), "aspects of older versions stay");
    assertTrue(spec.getAspectSpec("thingInfo").getPegasusSchema().contains("note"));
  }

  @Test
  public void testKeyAspectRenameIsRejected() throws Exception, EntityRegistryException {
    MergedEntityRegistry merged =
        new MergedEntityRegistry(registryOf()).apply(registryOf(thing(aspect("thingKey", KEY_V1))));

    EntityRegistryException e =
        expectThrows(
            EntityRegistryException.class,
            () -> merged.apply(registryOf(thing(aspect("otherKey", KEY_V1)))));
    assertTrue(e.getMessage().contains("key aspect"), e.getMessage());
    assertEquals(merged.getEntitySpec("thing").getKeyAspectName(), "thingKey");
  }

  @Test
  public void testKeyAspectSchemaChangeIsRejected() throws Exception, EntityRegistryException {
    MergedEntityRegistry merged =
        new MergedEntityRegistry(registryOf()).apply(registryOf(thing(aspect("thingKey", KEY_V1))));

    EntityRegistryException e =
        expectThrows(
            EntityRegistryException.class,
            () -> merged.apply(registryOf(thing(aspect("thingKey", KEY_V2)))));
    assertTrue(e.getMessage().contains("key aspect"), e.getMessage());
  }

  @Test
  public void testIncompatibleAspectIsRejected() throws Exception, EntityRegistryException {
    MergedEntityRegistry merged =
        new MergedEntityRegistry(registryOf())
            .apply(registryOf(thing(aspect("thingKey", KEY_V1), aspect("thingInfo", INFO_V1))));

    EntityRegistryException e =
        expectThrows(
            EntityRegistryException.class,
            () ->
                merged.apply(
                    registryOf(
                        thing(
                            aspect("thingKey", KEY_V1), aspect("thingInfo", INFO_INCOMPATIBLE)))));
    assertTrue(e.getMessage().contains("thingInfo schema is not compatible"), e.getMessage());
  }

  private static EntitySpec thing(AspectSpec... aspects) {
    return new PartialEntitySpec(
        List.of(aspects), new EntityAnnotation("thing", aspects[0].getName()));
  }

  private static AspectSpec aspect(String name, String schema) {
    return new AspectSpec(
        new AspectAnnotation(name, false, false, null, 1L, false),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        (RecordDataSchema) DataTemplateUtil.parseSchema(schema),
        null);
  }

  private static EntityRegistry registryOf(EntitySpec... entitySpecs) {
    Map<String, EntitySpec> specs = new HashMap<>();
    for (EntitySpec spec : entitySpecs) {
      specs.put(spec.getName().toLowerCase(), spec);
    }
    return new EntityRegistry() {
      @Nonnull
      @Override
      public EntitySpec getEntitySpec(@Nonnull String entityName) {
        return specs.get(entityName.toLowerCase());
      }

      @Nonnull
      @Override
      public EventSpec getEventSpec(@Nonnull String eventName) {
        throw new IllegalArgumentException(eventName);
      }

      @Nonnull
      @Override
      public Map<String, EntitySpec> getEntitySpecs() {
        return specs;
      }

      @Nonnull
      @Override
      public Map<String, AspectSpec> getAspectSpecs() {
        return new HashMap<>();
      }

      @Nonnull
      @Override
      public Map<String, EventSpec> getEventSpecs() {
        return Collections.emptyMap();
      }

      @Nonnull
      @Override
      public AspectTemplateEngine getAspectTemplateEngine() {
        return new AspectTemplateEngine();
      }
    };
  }
}
