package com.linkedin.metadata.timeline;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Guards the catalog-entity timeline registrations. Without them getTimeline throws
 * UnsupportedOperationException for the entity's urns, which the UI surfaces as a missing Change
 * History menu rather than an error. The expected aspect sets mirror what each entity carries in
 * the entity registry.
 */
public class TimelineServiceCatalogEntityRegistrationTest {

  private static TimelineServiceImpl newService() {
    return new TimelineServiceImpl(mock(AspectDao.class), mock(EntityRegistry.class));
  }

  @DataProvider(name = "documentationAspects")
  public Object[][] documentationAspects() {
    return new Object[][] {
      {
        DATA_FLOW_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            DATA_FLOW_INFO_ASPECT_NAME,
            EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME)
      },
      {
        DATA_JOB_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            DATA_JOB_INFO_ASPECT_NAME,
            EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME)
      },
      {
        CHART_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            CHART_INFO_ASPECT_NAME,
            EDITABLE_CHART_PROPERTIES_ASPECT_NAME)
      },
      {
        DASHBOARD_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            DASHBOARD_INFO_ASPECT_NAME,
            EDITABLE_DASHBOARD_PROPERTIES_ASPECT_NAME)
      },
      {METRIC_ENTITY_NAME, Set.of(INSTITUTIONAL_MEMORY_ASPECT_NAME, METRIC_INFO_ASPECT_NAME)},
      {
        SEMANTIC_MODEL_ENTITY_NAME,
        Set.of(INSTITUTIONAL_MEMORY_ASPECT_NAME, SEMANTIC_MODEL_INFO_ASPECT_NAME)
      },
      {
        ML_MODEL_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            ML_MODEL_PROPERTIES_ASPECT_NAME,
            ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME)
      },
      {
        ML_MODEL_GROUP_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME,
            ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME)
      },
      {
        ML_FEATURE_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            ML_FEATURE_PROPERTIES_ASPECT_NAME,
            ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME)
      },
      {
        ML_FEATURE_TABLE_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            ML_FEATURE_TABLE_PROPERTIES_ASPECT_NAME,
            ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME)
      },
      {
        ML_PRIMARY_KEY_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            ML_PRIMARY_KEY_PROPERTIES_ASPECT_NAME,
            ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME)
      },
      {
        CONTAINER_ENTITY_NAME,
        Set.of(
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            CONTAINER_PROPERTIES_ASPECT_NAME,
            CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME)
      },
      {
        GLOSSARY_NODE_ENTITY_NAME,
        Set.of(INSTITUTIONAL_MEMORY_ASPECT_NAME, GLOSSARY_NODE_INFO_ASPECT_NAME)
      },
      {
        APPLICATION_ENTITY_NAME,
        Set.of(INSTITUTIONAL_MEMORY_ASPECT_NAME, APPLICATION_PROPERTIES_ASPECT_NAME)
      },
      {
        REPOSITORY_ENTITY_NAME,
        Set.of(INSTITUTIONAL_MEMORY_ASPECT_NAME, REPOSITORY_PROPERTIES_ASPECT_NAME)
      },
      {AI_AGENT_ENTITY_NAME, Set.of(INSTITUTIONAL_MEMORY_ASPECT_NAME, AI_AGENT_INFO_ASPECT_NAME)},
      {
        AGENT_SKILL_ENTITY_NAME,
        Set.of(INSTITUTIONAL_MEMORY_ASPECT_NAME, AGENT_SKILL_INFO_ASPECT_NAME)
      },
      {API_ENTITY_NAME, Set.of(INSTITUTIONAL_MEMORY_ASPECT_NAME, API_PROPERTIES_ASPECT_NAME)}
    };
  }

  @Test(dataProvider = "documentationAspects")
  public void testDocumentationWatchesDescriptionAspects(String entityType, Set<String> expected) {
    Set<String> aspects =
        newService().getAspectsFromElements(entityType, Set.of(ChangeCategory.DOCUMENTATION));
    assertEquals(aspects, expected, entityType);
  }

  @DataProvider(name = "catalogAspects")
  public Object[][] catalogAspects() {
    return new Object[][] {
      {
        DATA_FLOW_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        DATA_JOB_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        CHART_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        DASHBOARD_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        METRIC_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        SEMANTIC_MODEL_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        ML_MODEL_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME,
            VERSION_PROPERTIES_ASPECT_NAME)
      },
      {
        ML_MODEL_GROUP_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        ML_FEATURE_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        ML_FEATURE_TABLE_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        ML_PRIMARY_KEY_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        CONTAINER_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        GLOSSARY_NODE_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            APPLICATION_MEMBERSHIP_ASPECT_NAME)
      },
      {
        APPLICATION_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME)
      },
      {
        REPOSITORY_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME)
      },
      {
        AI_AGENT_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            VERSION_PROPERTIES_ASPECT_NAME)
      },
      {
        AGENT_SKILL_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            VERSION_PROPERTIES_ASPECT_NAME)
      },
      {
        API_ENTITY_NAME,
        Set.of(
            OWNERSHIP_ASPECT_NAME,
            GLOBAL_TAGS_ASPECT_NAME,
            GLOSSARY_TERMS_ASPECT_NAME,
            DOMAINS_ASPECT_NAME,
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            VERSION_PROPERTIES_ASPECT_NAME)
      }
    };
  }

  @Test(dataProvider = "catalogAspects")
  public void testCatalogCategoriesCoverEntityAspects(String entityType, Set<String> expected) {
    Set<String> aspects =
        newService()
            .getAspectsFromElements(
                entityType,
                Set.of(
                    ChangeCategory.OWNERSHIP,
                    ChangeCategory.TAG,
                    ChangeCategory.GLOSSARY_TERM,
                    ChangeCategory.DOMAIN,
                    ChangeCategory.STRUCTURED_PROPERTY,
                    ChangeCategory.APPLICATION,
                    ChangeCategory.VERSIONING));
    for (String aspect : expected) {
      assertTrue(aspects.contains(aspect), entityType + " should watch " + aspect);
    }
  }
}
