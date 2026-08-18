package com.linkedin.metadata.timeline;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Verifies that the VERSIONING change category resolves to the versionProperties aspect for the
 * entity types wired to emit version milestones. Guards against a regression where an entity is
 * accidentally dropped from the timeline registry, which would silently return empty version
 * timelines rather than fail.
 */
public class TimelineServiceVersioningRegistrationTest {

  private static TimelineServiceImpl newService() {
    return new TimelineServiceImpl(mock(AspectDao.class), mock(EntityRegistry.class));
  }

  @Test
  public void testVersioningRegisteredForDatasetAndGlossaryTerm() {
    TimelineServiceImpl service = newService();

    for (String entityType : new String[] {DATASET_ENTITY_NAME, GLOSSARY_TERM_ENTITY_NAME}) {
      Set<String> aspects =
          service.getAspectsFromElements(entityType, Set.of(ChangeCategory.VERSIONING));
      assertTrue(
          aspects.contains(VERSION_PROPERTIES_ASPECT_NAME),
          entityType + " should map VERSIONING to " + VERSION_PROPERTIES_ASPECT_NAME);
    }
  }
}
