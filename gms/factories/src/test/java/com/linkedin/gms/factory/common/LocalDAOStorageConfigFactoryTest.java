package com.linkedin.gms.factory.common;

import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class LocalDAOStorageConfigFactoryTest {

  @Test
  public void testConfigFromAspectUnion() {
    LocalDAOStorageConfig storageConfig = LocalDAOStorageConfigFactory.getStorageConfig(EntityAspectUnion.class);
    assertEquals(storageConfig.getAspectStorageConfigMap().keySet(),
        new HashSet<>(Arrays.asList(AspectFoo.class, AspectBar.class)));
    assertNull(storageConfig.getAspectStorageConfigMap().get(AspectFoo.class));
    assertNull(storageConfig.getAspectStorageConfigMap().get(AspectBar.class));
  }

  @Test
  public void testReadResourceFile() {
    // read resource file with no config, only aspects
    LocalDAOStorageConfig storageConfig = LocalDAOStorageConfigFactory.readResourceFile(LocalDAOStorageConfigFactoryTest.class,
        "localDAOStorageEmptyConfig.json");
    assertEquals(new HashSet<>(Arrays.asList(AspectFoo.class, AspectBar.class)),
        storageConfig.getAspectStorageConfigMap().keySet());
    assertNull(storageConfig.getAspectStorageConfigMap().get(AspectFoo.class));
    assertNull(storageConfig.getAspectStorageConfigMap().get(AspectBar.class));

    // read config file that does not match the schema of LocalDAOStorageConfig
    LocalDAOStorageConfig storageConfig2 = LocalDAOStorageConfigFactory.readResourceFile(LocalDAOStorageConfigFactoryTest.class,
        "localDAOBadConfig.json");
    assertEquals(storageConfig2.getAspectStorageConfigMap(), Collections.emptyMap());
  }

  @Test
  public void testConfigFromInputFile() {
    LocalDAOStorageConfig storageConfig = LocalDAOStorageConfigFactory.getStorageConfig(LocalDAOStorageConfigFactoryTest.class,
        "localDAOStorageCompleteConfig.json");

    assertEquals(new HashSet<>(Arrays.asList(AspectFoo.class, AspectBar.class)),
        storageConfig.getAspectStorageConfigMap().keySet());
    LocalDAOStorageConfig.PathStorageConfig pathSpecStorageConfig1 = storageConfig.getAspectStorageConfigMap().get(AspectFoo.class)
        .getPathStorageConfigMap().get(AspectFoo.fields().value().toString());
    LocalDAOStorageConfig.PathStorageConfig pathSpecStorageConfig2 = storageConfig.getAspectStorageConfigMap().get(AspectBar.class)
        .getPathStorageConfigMap().get(AspectBar.fields().value().toString());
    assertTrue(pathSpecStorageConfig1.isStrongConsistentSecondaryIndex());
    assertTrue(pathSpecStorageConfig2.isStrongConsistentSecondaryIndex());
  }

  @Test
  public void testConfigFromInputFileAspectUnion() {
    LocalDAOStorageConfig storageConfig = LocalDAOStorageConfigFactory.getStorageConfig(EntityAspectUnion.class,
        LocalDAOStorageConfigFactoryTest.class, "localDAOStorageSomeAspectsConfig.json");

    assertEquals(new HashSet<>(Arrays.asList(AspectFoo.class, AspectBar.class)),
        storageConfig.getAspectStorageConfigMap().keySet());
    LocalDAOStorageConfig.PathStorageConfig pathSpecStorageConfig = storageConfig.getAspectStorageConfigMap().get(AspectFoo.class)
        .getPathStorageConfigMap().get(AspectFoo.fields().value().toString());
    assertTrue(pathSpecStorageConfig.isStrongConsistentSecondaryIndex());
    assertNull(storageConfig.getAspectStorageConfigMap().get(AspectBar.class));

    LocalDAOStorageConfig expectedStorageConfig = LocalDAOStorageConfigFactory.getStorageConfig(EntityAspectUnion.class,
        LocalDAOStorageConfigFactoryTest.class, "localDAOStorageMergedAspectsConfig.json");
    assertEquals(expectedStorageConfig, storageConfig);
  }

}