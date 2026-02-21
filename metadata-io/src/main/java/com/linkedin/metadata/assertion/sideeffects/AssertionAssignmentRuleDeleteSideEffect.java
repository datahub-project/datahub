package com.linkedin.metadata.assertion.sideeffects;

import static com.linkedin.metadata.Constants.TEST_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.service.util.AssertionAssignmentRuleUtils;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Side effect that deletes the backing metadata test entity when an assertion assignment rule is
 * deleted. This ensures cleanup happens transactionally regardless of which API path triggers the
 * deletion.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class AssertionAssignmentRuleDeleteSideEffect extends MCPSideEffect {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    return changeMCPS.stream().flatMap(item -> generateTestEntityDeletes(item, retrieverContext));
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  private static Stream<ChangeMCP> generateTestEntityDeletes(
      @Nonnull final ChangeMCP item, @Nonnull final RetrieverContext retrieverContext) {
    final Urn ruleUrn = item.getUrn();
    final Urn testUrn =
        AssertionAssignmentRuleUtils.createTestUrnForAssertionAssignmentRule(ruleUrn);
    final EntitySpec testSpec =
        retrieverContext.getAspectRetriever().getEntityRegistry().getEntitySpec(TEST_ENTITY_NAME);
    return testSpec.getAspectSpecs().stream()
        .map(
            aspectSpec ->
                DeleteItemImpl.builder()
                    .urn(testUrn)
                    .aspectName(aspectSpec.getName())
                    .auditStamp(item.getAuditStamp())
                    .build(retrieverContext.getAspectRetriever()));
  }
}
