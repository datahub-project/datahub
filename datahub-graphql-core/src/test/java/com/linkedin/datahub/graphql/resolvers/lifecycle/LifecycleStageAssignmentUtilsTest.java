package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import java.util.List;
import org.testng.annotations.Test;

public class LifecycleStageAssignmentUtilsTest {

  private static final Urn DRAFT = UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT");

  @Test
  public void testDraftIsAssignableToGlossaryButNotDocuments() {
    LifecycleStageTypeInfo info =
        new LifecycleStageTypeInfo()
            .setName("Draft")
            .setEntityTypes(new StringArray(List.of("document", "glossaryTerm")))
            .setSettings(new LifecycleStageSettings().setUserAssignable(true));

    assertTrue(
        LifecycleStageAssignmentUtils.isUserAssignableForEntity(DRAFT, info, "glossaryTerm"));
    assertFalse(LifecycleStageAssignmentUtils.isUserAssignableForEntity(DRAFT, info, "document"));
  }

  @Test
  public void testSystemManagedStageIsNeverUserAssignable() {
    LifecycleStageTypeInfo info =
        new LifecycleStageTypeInfo()
            .setName("System managed")
            .setSettings(new LifecycleStageSettings().setUserAssignable(false));

    assertFalse(LifecycleStageAssignmentUtils.isUserAssignableForEntity(DRAFT, info, "aiAgent"));
  }
}
