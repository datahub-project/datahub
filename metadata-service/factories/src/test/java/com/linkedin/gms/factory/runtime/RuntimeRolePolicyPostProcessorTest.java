package com.linkedin.gms.factory.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.runtime.RuntimeRole;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.support.SpringFactoriesLoader;
import org.springframework.mock.env.MockEnvironment;
import org.testng.annotations.Test;

public class RuntimeRolePolicyPostProcessorTest {

  private final RuntimeRolePolicyPostProcessor processor = new RuntimeRolePolicyPostProcessor();

  @Test
  public void registeredForSpringBootStartup() {
    assertTrue(
        SpringFactoriesLoader.loadFactoryNames(
                EnvironmentPostProcessor.class,
                RuntimeRolePolicyPostProcessor.class.getClassLoader())
            .contains(RuntimeRolePolicyPostProcessor.class.getName()));
  }

  @Test
  public void missingRoleIsServiceAndDoesNotOverlay() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("datahub.gms.entityGraphCache.enabled", "true");

    processor.postProcessEnvironment(environment, new SpringApplication());

    assertEquals(environment.getProperty(RuntimeRole.PROPERTY), null);
    assertEquals(environment.getProperty("datahub.gms.entityGraphCache.enabled"), "true");
    assertFalse(environment.getPropertySources().contains("runtimeRolePolicy"));
  }

  @Test
  public void clientForcesDistributedCachePropertiesOff() {
    MockEnvironment environment = enablingEnvironment("client");

    processor.postProcessEnvironment(environment, new SpringApplication());

    assertForcedOff(environment);
  }

  @Test
  public void upgradeForcesDistributedCachePropertiesOff() {
    MockEnvironment environment = enablingEnvironment("upgrade");

    processor.postProcessEnvironment(environment, new SpringApplication());

    assertForcedOff(environment);
  }

  @Test
  public void serviceHonorsEntityGraphCacheEnabled() {
    MockEnvironment environment = enablingEnvironment("service");

    processor.postProcessEnvironment(environment, new SpringApplication());

    assertEquals(environment.getProperty("datahub.gms.entityGraphCache.enabled"), "true");
    assertEquals(environment.getProperty("searchService.cacheImplementation"), "hazelcast");
    assertFalse(environment.getPropertySources().contains("runtimeRolePolicy"));
  }

  @Test
  public void overlayBeatsAnEarlierPropertySource() {
    StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addLast(new MapPropertySource("helm", enablingValues("CLIENT")));

    processor.postProcessEnvironment(environment, new SpringApplication());

    assertForcedOff(environment);
    assertEquals(
        environment
            .getPropertySources()
            .precedenceOf(
                environment
                    .getPropertySources()
                    .get(RuntimeRolePolicyPostProcessor.PROPERTY_SOURCE_NAME)),
        0);
  }

  @Test
  public void invalidRoleFailsStartup() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(RuntimeRole.PROPERTY, "gms");

    IllegalStateException error =
        expectThrows(
            IllegalStateException.class,
            () -> processor.postProcessEnvironment(environment, new SpringApplication()));
    assertEquals(
        error.getMessage(),
        "datahub.runtime.role must be service, client, or upgrade, but was 'gms'");
  }

  private static MockEnvironment enablingEnvironment(String role) {
    MockEnvironment environment = new MockEnvironment();
    enablingValues(role).forEach(environment::setProperty);
    return environment;
  }

  private static java.util.Map<String, Object> enablingValues(String role) {
    java.util.Map<String, Object> values = new java.util.LinkedHashMap<>();
    values.put(RuntimeRole.PROPERTY, role);
    values.put("datahub.gms.entityGraphCache.enabled", "true");
    values.put("searchService.cacheImplementation", "hazelcast");
    values.put("featureFlags.retentionBufferEnabled", "true");
    values.put("datahub.gms.rateLimits.endpoint.enabled", "true");
    values.put("datahub.gms.rateLimits.scoped.enabled", "true");
    values.put("ebean.entityWriteLockBackend", "hazelcast");
    values.put("datahub.usage.aggregation.enabled", "true");
    return values;
  }

  private static void assertForcedOff(org.springframework.core.env.Environment environment) {
    for (RuntimeRole.ForcedProperty override : RuntimeRole.DISTRIBUTED_CACHE_OVERRIDES) {
      assertEquals(environment.getProperty(override.key()), override.value(), override.key());
    }
  }
}
