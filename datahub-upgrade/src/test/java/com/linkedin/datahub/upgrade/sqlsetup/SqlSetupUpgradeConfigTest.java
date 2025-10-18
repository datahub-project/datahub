package com.linkedin.datahub.upgrade.sqlsetup;

import static org.testng.Assert.assertNotNull;

import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.testng.annotations.Test;

@TestPropertySource(properties = {"entityService.impl=ebean"})
public class SqlSetupUpgradeConfigTest {

  @Test
  public void testSqlSetupUpgradeConfig() {
    SqlSetupUpgradeConfig config = new SqlSetupUpgradeConfig();

    assertNotNull(config);

    // Verify that the class is properly annotated
    Configuration annotation = config.getClass().getAnnotation(Configuration.class);
    assertNotNull(annotation);
  }

  @Test
  public void testConfigurationAnnotations() {
    Class<?> configClass = SqlSetupUpgradeConfig.class;

    // Verify @Configuration annotation
    assertNotNull(configClass.getAnnotation(Configuration.class));

    // Verify @Import annotation
    assertNotNull(configClass.getAnnotation(org.springframework.context.annotation.Import.class));

    // Verify @ComponentScan annotation
    assertNotNull(
        configClass.getAnnotation(org.springframework.context.annotation.ComponentScan.class));
  }
}
