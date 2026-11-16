package com.linkedin.metadata.timeline;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.util.Set;
import org.testng.annotations.Test;

/** Guards the LIFECYCLE category for the entity types whose stage changes appear in history. */
public class TimelineServiceLifecycleRegistrationTest {

  @Test
  public void testLifecycleRegisteredForDocumentAndGlossaryTerm() {
    TimelineServiceImpl service =
        new TimelineServiceImpl(mock(AspectDao.class), mock(EntityRegistry.class));

    for (String entityType : new String[] {DOCUMENT_ENTITY_NAME, GLOSSARY_TERM_ENTITY_NAME}) {
      Set<String> aspects =
          service.getAspectsFromElements(entityType, Set.of(ChangeCategory.LIFECYCLE));
      assertTrue(
          aspects.contains(STATUS_ASPECT_NAME),
          entityType + " should map LIFECYCLE to " + STATUS_ASPECT_NAME);
    }
  }
}
