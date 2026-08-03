package com.linkedin.datahub.upgrade.system.postgres;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.LinkedHashSet;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgSearchEntitySearchGroupRegistrySeedStepTest {

  @Mock private EntityRegistry mockEntityRegistry;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testResolveSearchGroupsIncludesDefaultAndRegistry() {
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Set.of("primary", "timeseries"));
    LinkedHashSet<String> groups =
        PgSearchEntitySearchGroupRegistrySeedStep.resolveSearchGroups(mockEntityRegistry);
    assertEquals(groups.size(), 3);
    assertTrue(groups.contains(EntityAnnotation.DEFAULT_SEARCH_GROUP));
    assertTrue(groups.contains("primary"));
    assertTrue(groups.contains("timeseries"));
  }
}
