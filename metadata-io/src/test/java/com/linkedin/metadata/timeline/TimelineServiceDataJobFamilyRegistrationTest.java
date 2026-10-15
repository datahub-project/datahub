package com.linkedin.metadata.timeline;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Guards the dataFlow / dataJob timeline registrations. Without them getTimeline throws
 * UnsupportedOperationException for pipeline and task urns, which the UI surfaces as a missing
 * Change History menu rather than an error.
 */
public class TimelineServiceDataJobFamilyRegistrationTest {

  private static TimelineServiceImpl newService() {
    return new TimelineServiceImpl(mock(AspectDao.class), mock(EntityRegistry.class));
  }

  @Test
  public void testDataFlowDocumentationWatchesInfoAndEditableAspects() {
    Set<String> aspects =
        newService()
            .getAspectsFromElements(DATA_FLOW_ENTITY_NAME, Set.of(ChangeCategory.DOCUMENTATION));
    assertEquals(
        aspects,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            DATA_FLOW_INFO_ASPECT_NAME,
            EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME));
  }

  @Test
  public void testDataJobDocumentationWatchesInfoAndEditableAspects() {
    Set<String> aspects =
        newService()
            .getAspectsFromElements(DATA_JOB_ENTITY_NAME, Set.of(ChangeCategory.DOCUMENTATION));
    assertEquals(
        aspects,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            DATA_JOB_INFO_ASPECT_NAME,
            EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME));
  }

  @Test
  public void testCatalogCategoriesRegisteredForDataFlowAndDataJob() {
    TimelineServiceImpl service = newService();
    for (String entityType : new String[] {DATA_FLOW_ENTITY_NAME, DATA_JOB_ENTITY_NAME}) {
      Set<String> aspects =
          service.getAspectsFromElements(
              entityType,
              Set.of(
                  ChangeCategory.OWNERSHIP,
                  ChangeCategory.TAG,
                  ChangeCategory.GLOSSARY_TERM,
                  ChangeCategory.DOMAIN,
                  ChangeCategory.STRUCTURED_PROPERTY,
                  ChangeCategory.APPLICATION));
      for (String expected :
          new String[] {
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME
          }) {
        assertTrue(aspects.contains(expected), entityType + " should watch " + expected);
      }
    }
  }
}
