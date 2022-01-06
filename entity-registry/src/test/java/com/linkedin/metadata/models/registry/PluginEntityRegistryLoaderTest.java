package com.linkedin.metadata.models.registry;

import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.registry.config.EntityRegistryLoadResult;
import com.linkedin.metadata.models.registry.config.LoadStatus;
import com.linkedin.util.Pair;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.testng.annotations.Test;

import static com.linkedin.metadata.models.registry.TestConstants.*;
import static org.testng.Assert.*;


public class PluginEntityRegistryLoaderTest {

  @Test
  public void testEntityRegistry() throws FileNotFoundException, InterruptedException {
    EntityRegistry baseEntityRegistry = new EntityRegistry() {
      @Nonnull
      @Override
      public EntitySpec getEntitySpec(@Nonnull String entityName) {
        return null;
      }

      @Nonnull
      @Override
      public Map<String, EntitySpec> getEntitySpecs() {
        return null;
      }
    };

    MergedEntityRegistry configEntityRegistry = new MergedEntityRegistry(baseEntityRegistry);
    PluginEntityRegistryLoader pluginEntityRegistryLoader =
        new PluginEntityRegistryLoader(TestConstants.BASE_DIRECTORY).withBaseRegistry(configEntityRegistry).start(true);
    assertEquals(pluginEntityRegistryLoader.getPatchRegistries().size(), 1);
    EntityRegistryLoadResult loadResult =
        pluginEntityRegistryLoader.getPatchRegistries().get(TestConstants.TEST_REGISTRY).get(TEST_VERSION).getSecond();
    assertNotNull(loadResult);
    assertEquals(loadResult.getLoadResult(), LoadStatus.FAILURE);
  }

  private EntityRegistry getBaseEntityRegistry() {
    final AspectSpec keyAspectSpec =
        new AspectSpec(new AspectAnnotation("datasetKey", false, false, null), Collections.emptyList(), Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList(),
            (RecordDataSchema) DataSchemaFactory.getInstance().getAspectSchema("datasetKey").get(),
            DataSchemaFactory.getInstance().getAspectClass("datasetKey").get());

    final Map<String, EntitySpec> entitySpecMap = new HashMap<>(1);
    List<AspectSpec> aspectSpecList = new ArrayList<>(1);
    aspectSpecList.add(keyAspectSpec);
    EntitySpec baseEntitySpec = new DefaultEntitySpec(aspectSpecList, new EntityAnnotation("dataset", "datasetKey"),
        (RecordDataSchema) DataSchemaFactory.getInstance().getEntitySchema("dataset").get());

    entitySpecMap.put("dataset", baseEntitySpec);

    EntityRegistry baseEntityRegistry = new EntityRegistry() {

      @Nonnull
      @Override
      public EntitySpec getEntitySpec(@Nonnull String entityName) {
        assertEquals(entityName, "dataset");
        return baseEntitySpec;
      }

      @Nonnull
      @Override
      public Map<String, EntitySpec> getEntitySpecs() {
        return entitySpecMap;
      }
    };
    return baseEntityRegistry;
  }

  @Test
  public void testEntityRegistryWithGoodBase() throws FileNotFoundException, InterruptedException {

    MergedEntityRegistry configEntityRegistry = new MergedEntityRegistry(getBaseEntityRegistry());
    PluginEntityRegistryLoader pluginEntityRegistryLoader =
        new PluginEntityRegistryLoader(BASE_DIRECTORY).withBaseRegistry(configEntityRegistry).start(true);
    assertEquals(pluginEntityRegistryLoader.getPatchRegistries().size(), 1);
    EntityRegistryLoadResult loadResult =
        pluginEntityRegistryLoader.getPatchRegistries().get(TEST_REGISTRY).get(TEST_VERSION).getSecond();
    assertNotNull(loadResult);
    assertEquals(loadResult.getLoadResult(), LoadStatus.SUCCESS, "load failed with " + loadResult.getFailureReason());

    Map<String, EntitySpec> entitySpecs = configEntityRegistry.getEntitySpecs();
    EntitySpec entitySpec = configEntityRegistry.getEntitySpec("dataset");
    assertEquals(entitySpec.getName(), "dataset");
    assertEquals(entitySpec.getKeyAspectSpec().getName(), "datasetKey");
    Optional<DataSchema> dataSchema =
        Optional.ofNullable(entitySpecs.get("dataset").getAspectSpec("datasetKey").getPegasusSchema());
    assertTrue(dataSchema.isPresent(), "datasetKey");
    assertNotNull(entitySpec.getAspectSpec("testDataQualityRules"));
    assertEquals(entitySpecs.values().size(), 1);
    assertEquals(entitySpec.getAspectSpecs().size(), 2);
  }

  @Test
  /**
   * Tests that we can load up entity registries that represent safe evolutions as well as decline to load registries that represent unsafe evolutions.
   *
   */ public void testEntityRegistryVersioning() throws InterruptedException {
    MergedEntityRegistry configEntityRegistry = new MergedEntityRegistry(getBaseEntityRegistry());
    String multiversionPluginDir = "src/test_plugins/";

    PluginEntityRegistryLoader pluginEntityRegistryLoader =
        new PluginEntityRegistryLoader(multiversionPluginDir).withBaseRegistry(configEntityRegistry).start(true);
    Map<String, Map<ComparableVersion, Pair<EntityRegistry, EntityRegistryLoadResult>>> loadedRegistries =
        pluginEntityRegistryLoader.getPatchRegistries();

    String registryName = "mycompany-dq-model";
    assertTrue(loadedRegistries.containsKey(registryName));
    assertTrue(loadedRegistries.get(registryName).containsKey(new ComparableVersion("0.0.1")));
    assertEquals(loadedRegistries.get(registryName).get(new ComparableVersion("0.0.1")).getSecond().getLoadResult(),
        LoadStatus.SUCCESS);
    assertEquals(loadedRegistries.get(registryName).get(new ComparableVersion("0.0.2")).getSecond().getLoadResult(),
        LoadStatus.SUCCESS);
    assertEquals(loadedRegistries.get(registryName).get(new ComparableVersion("0.0.3")).getSecond().getLoadResult(),
        LoadStatus.FAILURE);
    assertTrue(loadedRegistries.get(registryName)
        .get(new ComparableVersion("0.0.3"))
        .getSecond()
        .getFailureReason()
        .contains("new record removed required fields type"));

    assertTrue(configEntityRegistry.getEntitySpec("dataset").hasAspect("dataQualityRules"));
    EntitySpec datasetSpec = configEntityRegistry.getEntitySpec("dataset");
    RecordDataSchema dataSchema =
        configEntityRegistry.getEntitySpec("dataset").getAspectSpec("dataQualityRules").getPegasusSchema();
    ArrayDataSchema arrayDataSchema =
        (ArrayDataSchema) dataSchema.getField("rules").getType().getDereferencedDataSchema();
    // Aspect Schema should be the same as version 0.0.2, checking to see that all fields exist
    RecordDataSchema innerSchema = (RecordDataSchema) arrayDataSchema.getItems();
    assertEquals(innerSchema.getFields().size(), 4);
    assertTrue(innerSchema.contains("field"));
    assertTrue(innerSchema.contains("type"));
    assertTrue(innerSchema.contains("checkDefinition"));
    assertTrue(innerSchema.contains("url"));
  }
}
