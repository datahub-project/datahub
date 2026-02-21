package com.linkedin.metadata.assertion.sideeffects;

import static com.linkedin.metadata.Constants.TEST_ENTITY_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.util.AssertionAssignmentRuleUtils;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AssertionAssignmentRuleDeleteSideEffectTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(AssertionAssignmentRuleDeleteSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(List.of("DELETE"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName("assertionAssignmentRule")
                      .aspectName("assertionAssignmentRuleKey")
                      .build()))
          .build();

  private static final Urn RULE_URN = UrnUtils.getUrn("urn:li:assertionAssignmentRule:test123");
  private static final AuditStamp AUDIT_STAMP =
      new AuditStamp()
          .setTime(System.currentTimeMillis())
          .setActor(UrnUtils.getUrn("urn:li:corpuser:test"));

  private CachingAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(CachingAspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);

    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(GraphRetriever.EMPTY)
            .build();
  }

  @Test
  public void testDeleteRuleEmitsTestEntityDeletes() {
    AssertionAssignmentRuleDeleteSideEffect sideEffect =
        new AssertionAssignmentRuleDeleteSideEffect();
    sideEffect.setConfig(TEST_PLUGIN_CONFIG);

    DeleteItemImpl inputItem =
        DeleteItemImpl.builder()
            .urn(RULE_URN)
            .aspectName("assertionAssignmentRuleKey")
            .auditStamp(AUDIT_STAMP)
            .build(mockAspectRetriever);

    List<ChangeMCP> result =
        sideEffect
            .applyMCPSideEffect(List.of(inputItem), retrieverContext)
            .collect(Collectors.toList());

    Urn expectedTestUrn =
        AssertionAssignmentRuleUtils.createTestUrnForAssertionAssignmentRule(RULE_URN);
    EntitySpec testSpec = TEST_REGISTRY.getEntitySpec(TEST_ENTITY_NAME);
    int expectedAspectCount = testSpec.getAspectSpecs().size();
    Set<String> expectedAspectNames =
        testSpec.getAspectSpecs().stream().map(spec -> spec.getName()).collect(Collectors.toSet());

    assertEquals(result.size(), expectedAspectCount);
    assertTrue(result.stream().allMatch(item -> item.getUrn().equals(expectedTestUrn)));
    assertTrue(result.stream().allMatch(item -> item instanceof DeleteItemImpl));
    assertTrue(result.stream().allMatch(item -> item.getChangeType() == ChangeType.DELETE));

    Set<String> actualAspectNames =
        result.stream().map(ChangeMCP::getAspectName).collect(Collectors.toSet());
    assertEquals(actualAspectNames, expectedAspectNames);
  }

  @Test
  public void testEmptyInputReturnsNoDeletes() {
    AssertionAssignmentRuleDeleteSideEffect sideEffect =
        new AssertionAssignmentRuleDeleteSideEffect();
    sideEffect.setConfig(TEST_PLUGIN_CONFIG);

    List<ChangeMCP> result =
        sideEffect.applyMCPSideEffect(List.of(), retrieverContext).collect(Collectors.toList());

    assertTrue(result.isEmpty());
  }
}
