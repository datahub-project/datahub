package com.linkedin.metadata.timeline;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.PropertiesDescriptionChangeEventGenerator;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TimelineServiceGlossaryRegistrationTest {

  private static TimelineServiceImpl newService() {
    return new TimelineServiceImpl(mock(AspectDao.class), mock(EntityRegistry.class));
  }

  @Test
  public void testGlossaryNodeInfoTrackedForDocumentationAndParent() {
    TimelineServiceImpl service = newService();
    assertTrue(
        service
            .getAspectsFromElements(GLOSSARY_NODE_ENTITY_NAME, Set.of(ChangeCategory.DOCUMENTATION))
            .contains(GLOSSARY_NODE_INFO_ASPECT_NAME));
    assertTrue(
        service
            .getAspectsFromElements(GLOSSARY_NODE_ENTITY_NAME, Set.of(ChangeCategory.PARENT))
            .contains(GLOSSARY_NODE_INFO_ASPECT_NAME));
  }

  @Test
  public void testGlossaryTermInfoTrackedForParent() {
    assertTrue(
        newService()
            .getAspectsFromElements(GLOSSARY_TERM_ENTITY_NAME, Set.of(ChangeCategory.PARENT))
            .contains(GLOSSARY_TERM_INFO_ASPECT_NAME));
  }

  @Test
  public void testGlossaryTermDocumentationUsesMultiFieldGenerator() {
    EntityChangeEventGenerator<?> generator =
        newService()
            .getTimelineGenerator(
                GLOSSARY_TERM_ENTITY_NAME,
                ChangeCategory.DOCUMENTATION,
                GLOSSARY_TERM_INFO_ASPECT_NAME);
    assertTrue(generator instanceof PropertiesDescriptionChangeEventGenerator);
    assertEquals(
        ((PropertiesDescriptionChangeEventGenerator<?>) generator).getTrackedFieldNames(),
        Set.of("name", "definition", "customProperties", "termSource", "sourceRef", "sourceUrl"));
  }

  @DataProvider(name = "deprecationEntities")
  public Object[][] deprecationEntities() {
    return new Object[][] {
      {DATASET_ENTITY_NAME},
      {GLOSSARY_TERM_ENTITY_NAME},
      {DOMAIN_ENTITY_NAME},
      {DATA_PRODUCT_ENTITY_NAME},
      {CONTAINER_ENTITY_NAME},
      {CHART_ENTITY_NAME}
    };
  }

  @Test(dataProvider = "deprecationEntities")
  public void testDeprecationRegistered(String entityType) {
    assertTrue(
        newService()
            .getAspectsFromElements(entityType, Set.of(ChangeCategory.DEPRECATION))
            .contains(DEPRECATION_ASPECT_NAME));
  }
}
