package com.linkedin.metadata.aspect.plugins.hooks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Optional;
import org.testng.annotations.Test;

public class EntityEligibleSideEffectTest {

  @Test
  public void testAnEntityTypeTheRegistryDoesNotKnowIsNeverEligible() {
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec("unknownType")).thenReturn(Optional.empty());

    EntityEligibleSideEffect acceptsEverything = spec -> true;

    assertFalse(acceptsEverything.isEntityTypeEligible("unknownType", registry));
  }

  @Test
  public void testAKnownEntityTypeFollowsTheEligibilityCheck() {
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec("document")).thenReturn(Optional.of(mock(EntitySpec.class)));

    EntityEligibleSideEffect accepts = spec -> true;
    EntityEligibleSideEffect rejects = spec -> false;

    assertTrue(accepts.isEntityTypeEligible("document", registry));
    assertFalse(rejects.isEntityTypeEligible("document", registry));
  }
}
