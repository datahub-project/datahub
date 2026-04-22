package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.LifecycleStageType;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import java.util.List;
import org.testng.annotations.Test;

public class LifecycleStageTypeMapperTest {

  private static final String URN = "urn:li:lifecycleStageType:DRAFT";

  private static AuditStamp makeStamp() {
    AuditStamp stamp = new AuditStamp();
    stamp.setTime(0L);
    stamp.setActor(UrnUtils.getUrn("urn:li:corpuser:system"));
    return stamp;
  }

  private static LifecycleStageTypeInfo makeInfo(String name, boolean hideInSearch) {
    LifecycleStageSettings settings = new LifecycleStageSettings();
    settings.setHideInSearch(hideInSearch);

    LifecycleStageTypeInfo info = new LifecycleStageTypeInfo();
    info.setName(name);
    info.setSettings(settings);
    info.setCreated(makeStamp());
    info.setLastModified(makeStamp());
    return info;
  }

  @Test
  public void testBasicMapping() {
    LifecycleStageTypeInfo info = makeInfo("Draft", true);
    info.setDescription("A draft stage");
    info.setEntityTypes(new StringArray(List.of("dataset", "document")));

    LifecycleStageType result = LifecycleStageTypeMapper.map(URN, info);

    assertEquals(result.getUrn(), URN);
    assertEquals(result.getName(), "Draft");
    assertEquals(result.getDescription(), "A draft stage");
    assertTrue(result.getHideInSearch());
    assertEquals(result.getEntityTypes(), List.of("dataset", "document"));
    assertNull(result.getAllowedPreviousStages());
  }

  @Test
  public void testMissingDescription() {
    LifecycleStageTypeInfo info = makeInfo("InReview", false);

    LifecycleStageType result = LifecycleStageTypeMapper.map(URN, info);

    assertNull(result.getDescription());
  }

  @Test
  public void testMissingEntityTypes() {
    LifecycleStageTypeInfo info = makeInfo("Archived", true);

    LifecycleStageType result = LifecycleStageTypeMapper.map(URN, info);

    assertNull(result.getEntityTypes());
  }

  @Test
  public void testTransitionPolicyWithAllowedPreviousStages() {
    LifecycleStageTypeInfo info = makeInfo("InReview", true);

    LifecycleStageTransitionPolicy policy = new LifecycleStageTransitionPolicy();
    policy.setAllowedPreviousStages(
        new UrnArray(
            List.of(
                UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT"),
                UrnUtils.getUrn("urn:li:lifecycleStageType:REJECTED"))));
    info.setTransitionPolicy(policy);

    LifecycleStageType result = LifecycleStageTypeMapper.map(URN, info);

    assertNotNull(result.getAllowedPreviousStages());
    assertEquals(result.getAllowedPreviousStages().size(), 2);
    assertTrue(result.getAllowedPreviousStages().contains("urn:li:lifecycleStageType:DRAFT"));
    assertTrue(result.getAllowedPreviousStages().contains("urn:li:lifecycleStageType:REJECTED"));
  }

  @Test
  public void testTransitionPolicyWithoutAllowedPreviousStages() {
    LifecycleStageTypeInfo info = makeInfo("Open", false);
    info.setTransitionPolicy(new LifecycleStageTransitionPolicy());

    LifecycleStageType result = LifecycleStageTypeMapper.map(URN, info);

    assertNull(result.getAllowedPreviousStages());
  }

  @Test
  public void testHideInSearchFalse() {
    LifecycleStageTypeInfo info = makeInfo("Deprecated", false);

    LifecycleStageType result = LifecycleStageTypeMapper.map(URN, info);

    assertEquals(result.getHideInSearch(), false);
  }
}
