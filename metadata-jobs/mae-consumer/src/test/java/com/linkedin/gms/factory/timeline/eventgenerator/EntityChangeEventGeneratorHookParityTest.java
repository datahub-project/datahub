package com.linkedin.gms.factory.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.kafka.hook.event.PlatformEventGeneratorHook;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.Test;

/**
 * Generators registered in {@link EntityChangeEventGeneratorRegistryFactory} only run for aspects
 * that {@link PlatformEventGeneratorHook} routes to them. A generator registered without a matching
 * hook entry never emits anything, and a hook entry without a generator is a no-op, so this test
 * keeps the two lists in sync.
 *
 * <p>The test lives in mae-consumer, which sees both the factory and the hook, and in the factory's
 * package so it can build the real registry through the bean method.
 */
public class EntityChangeEventGeneratorHookParityTest {

  /**
   * Generators registered in the factory that the hook deliberately does not route. Routing them
   * would publish change event types that are not emitted today.
   */
  private static final Set<String> NOT_ROUTED_BY_HOOK =
      Set.of(INSTITUTIONAL_MEMORY_ASPECT_NAME, GLOSSARY_TERM_INFO_ASPECT_NAME);

  /**
   * Aspects the hook routes although the factory registers no generator for them, so they produce
   * no change events.
   */
  private static final Set<String> ROUTED_WITHOUT_GENERATOR =
      Set.of(CONTAINER_PROPERTIES_ASPECT_NAME, CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME);

  @Test
  public void testHookAndGeneratorRegistryAgree() {
    final EntityChangeEventGeneratorRegistryFactory factory =
        new EntityChangeEventGeneratorRegistryFactory();
    final ApplicationContext applicationContext = mock(ApplicationContext.class);
    final SystemEntityClient entityClient = mock(SystemEntityClient.class);
    when(applicationContext.getBean(SystemEntityClient.class)).thenReturn(entityClient);
    factory.applicationContext = applicationContext;

    final EntityChangeEventGeneratorRegistry registry =
        factory.entityChangeEventGeneratorRegistry(mock(OperationContext.class), entityClient);
    final Set<String> registered = registry.getRegisteredAspectNames();
    final Set<String> routed = PlatformEventGeneratorHook.getSupportedAspectNames();

    assertTrue(
        registered.containsAll(NOT_ROUTED_BY_HOOK),
        "Stale exclusion: " + Sets.difference(NOT_ROUTED_BY_HOOK, registered));
    assertEquals(
        Sets.difference(Sets.difference(registered, routed), NOT_ROUTED_BY_HOOK),
        Set.of(),
        "Generators registered in EntityChangeEventGeneratorRegistryFactory but not routed by"
            + " PlatformEventGeneratorHook (add them to ENTITY_CHANGE_SUPPORTED_ASPECT_NAMES)");
    assertTrue(
        Sets.intersection(NOT_ROUTED_BY_HOOK, routed).isEmpty(),
        "Aspect is routed by the hook but still listed as not routed: "
            + Sets.intersection(NOT_ROUTED_BY_HOOK, routed));

    assertTrue(
        routed.containsAll(ROUTED_WITHOUT_GENERATOR),
        "Stale exclusion: " + Sets.difference(ROUTED_WITHOUT_GENERATOR, routed));
    assertEquals(
        Sets.difference(Sets.difference(routed, registered), ROUTED_WITHOUT_GENERATOR),
        Set.of(),
        "Aspects routed by PlatformEventGeneratorHook without a generator registered in"
            + " EntityChangeEventGeneratorRegistryFactory");
    assertTrue(
        Sets.intersection(ROUTED_WITHOUT_GENERATOR, registered).isEmpty(),
        "Aspect has a generator but is still listed as routed without one: "
            + Sets.intersection(ROUTED_WITHOUT_GENERATOR, registered));
  }
}
