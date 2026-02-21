package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.util.AssertionAssignmentRuleTestBuilder;
import com.linkedin.metadata.service.util.AssertionAssignmentRuleUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to execute CRUD operations around assertion assignment rules and their backing
 * automations (metadata tests).
 */
@Slf4j
public class AssertionAssignmentRuleService extends BaseService {

  public AssertionAssignmentRuleService(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
  }

  /** Upsert the backing metadata test automation for an assertion assignment rule. */
  public void upsertAssertionAssignmentRuleAutomation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn ruleUrn,
      @Nonnull final AssertionAssignmentRuleInfo ruleInfo) {
    final Urn testUrn =
        AssertionAssignmentRuleUtils.createTestUrnForAssertionAssignmentRule(ruleUrn);
    final TestInfo testInfo =
        AssertionAssignmentRuleTestBuilder.buildAssertionAssignmentRuleTest(
            opContext, ruleUrn, ruleInfo);
    try {
      final List<MetadataChangeProposal> changes =
          ImmutableList.of(
              AspectUtils.buildMetadataChangeProposal(testUrn, TEST_INFO_ASPECT_NAME, testInfo));
      ingestChangeProposals(opContext, changes, false);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Failed to upsert assertion assignment rule automation for rule with urn: %s",
              ruleUrn),
          e);
    }
  }

  /** Get the AssertionAssignmentRuleInfo aspect for a given rule urn. */
  @Nullable
  public AssertionAssignmentRuleInfo getRuleInfo(
      @Nonnull OperationContext opContext, @Nonnull final Urn ruleUrn) {
    try {
      final EntityResponse response =
          this.entityClient.getV2(
              opContext,
              ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME,
              ruleUrn,
              ImmutableSet.of(ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME)) {
        return new AssertionAssignmentRuleInfo(
            response
                .getAspects()
                .get(ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME)
                .getValue()
                .data());
      }
      return null;
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve assertion assignment rule info for rule with urn: %s", ruleUrn),
          e);
    }
  }

  /** List assertion assignment rules by query and filters. */
  public SearchResult listRules(
      @Nonnull OperationContext opContext,
      @Nonnull final String query,
      @Nullable final Filter filters,
      final int start,
      final int count) {
    try {
      return this.entityClient.search(
          opContext, ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME, query, filters, null, start, count);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(
          String.format(
              "Failed to list assertion assignment rules with query: '%s', start: %d, count: %d",
              query, start, count),
          e);
    }
  }
}
