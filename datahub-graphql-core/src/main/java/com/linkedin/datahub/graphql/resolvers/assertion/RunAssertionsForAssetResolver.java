package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.assertion.AssertionUtils.MAX_ASSERTIONS_TO_RUN_ON_DEMAND;
import static com.linkedin.datahub.graphql.resolvers.assertion.AssertionUtils.extractStringMapEntryInputList;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.RunAssertionsResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Submits a group of assertions to be run for an asset by the Monitors Service. */
public class RunAssertionsForAssetResolver
    implements DataFetcher<CompletableFuture<RunAssertionsResult>> {
  private final MonitorService monitorService;
  private final AssertionService assertionsService;

  public RunAssertionsForAssetResolver(
      @Nonnull final MonitorService monitorService,
      @Nonnull final AssertionService assertionService) {
    this.monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    this.assertionsService =
        Objects.requireNonNull(assertionService, "assertionService is required");
  }

  @Override
  public CompletableFuture<RunAssertionsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final Urn entityUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final Boolean saveResults = environment.getArgumentOrDefault("saveResults", true);
    final Boolean async = environment.getArgumentOrDefault("async", false);
    final List<String> tagUrnStrs =
        environment.getArgumentOrDefault("tagUrns", Collections.emptyList());
    final List<Urn> tagUrns =
        tagUrnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    final Map<String, String> parameters = extractStringMapEntryInputList(environment);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> runAssertionsForAsset(context, entityUrn, saveResults, tagUrns, parameters, async),
        this.getClass().getSimpleName(),
        "get");
  }

  private RunAssertionsResult runAssertionsForAsset(
      @Nonnull final QueryContext context,
      @Nonnull final Urn entityUrn,
      final boolean saveResults,
      @Nonnull final List<Urn> selectedTags,
      @Nonnull final Map<String, String> parameters,
      final boolean async) {
    final List<Urn> assertionUrns =
        extractAndValidateEntityAssertions(entityUrn, context, selectedTags);
    if (assertionUrns.size() > MAX_ASSERTIONS_TO_RUN_ON_DEMAND) {
      throw new DataHubGraphQLException(
          String.format(
              "Failed to run Assertions. Number of assertions to run exceeds the limit of %d.",
              MAX_ASSERTIONS_TO_RUN_ON_DEMAND),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    return runAssertions(context, assertionUrns, saveResults, parameters, async);
  }

  @Nullable
  private RunAssertionsResult runAssertions(
      @Nonnull final QueryContext context,
      @Nonnull final List<Urn> assertionUrns,
      final boolean saveResults,
      @Nonnull final Map<String, String> parameters,
      final boolean async) {
    /* Everything is valid, let's run the assertions */
    final Map<Urn, com.linkedin.assertion.AssertionResult> results =
        monitorService.runAssertions(
            assertionUrns,
            !saveResults, // Dry run is the opposite of saveResults.
            parameters,
            async);

    if (async) {
      // If the request is async, we return null to indicate that the result will be available
      // later.
      return null;
    }

    return AssertionUtils.extractRunResults(context, assertionUrns, results);
  }

  @Nonnull
  private List<Urn> extractAndValidateEntityAssertions(
      @Nonnull final Urn entityUrn,
      @Nonnull final QueryContext context,
      @Nonnull final List<Urn> selectedTags) {

    final List<Urn> assertionUrns =
        assertionsService.getAssertionUrnsForEntity(context.getOperationContext(), entityUrn);

    final List<Urn> finalAssertionUrns = new ArrayList<>();

    for (final Urn assertionUrn : assertionUrns) {
      final EntityResponse response =
          assertionsService.getAssertionEntityResponse(context.getOperationContext(), assertionUrn);
      final AssertionInfo info = extractAssertionInfo(response);
      final GlobalTags maybeTags = extractAssertionTags(response);
      if (info == null
          || !info.hasSource()
          || !AssertionSourceType.NATIVE.equals(info.getSource().getType())) {
        // Simply skip this assertion.
        continue;
      }
      if (!isSelectedByTags(selectedTags, maybeTags)) {
        // Skip this assertion if it does not have all the required tags.
        continue;
      }
      validateRunNativeAssertionPrivileges(info, context);
      finalAssertionUrns.add(assertionUrn);
    }

    return finalAssertionUrns;
  }

  private void validateRunNativeAssertionPrivileges(
      @Nonnull final AssertionInfo info, @Nonnull final QueryContext context) {
    final Urn asserteeUrn = AssertionUtils.getAsserteeUrnFromInfo(info);
    if (!AssertionUtils.isAuthorizedToRunAssertion(asserteeUrn, info.getType(), context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
  }

  private boolean isSelectedByTags(
      @Nonnull final List<Urn> selectedTags, @Nullable final GlobalTags assertionTags) {
    if (selectedTags.isEmpty()) {
      // If no selected tags were provided, then we should run the assertion.
      return true;
    }
    if (assertionTags == null) {
      // If the assertion does not have any tags, then we should not run the assertion.
      return false;
    }
    // If the assertion has ANY of the selected tags, then we should run the assertion.
    final List<String> selectedTagStrs =
        selectedTags.stream().map(Urn::toString).collect(Collectors.toList());
    final List<String> assertionTagStrs =
        assertionTags.getTags().stream()
            .map(tag -> tag.getTag().toString())
            .collect(Collectors.toList());
    return assertionTagStrs.stream().anyMatch(selectedTagStrs::contains);
  }

  @Nullable
  private AssertionInfo extractAssertionInfo(@Nullable final EntityResponse response) {
    if (response != null
        && response.getAspects().containsKey(Constants.ASSERTION_INFO_ASPECT_NAME)) {
      return new AssertionInfo(
          response.getAspects().get(Constants.ASSERTION_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  private GlobalTags extractAssertionTags(@Nullable final EntityResponse response) {
    if (response != null && response.getAspects().containsKey(Constants.GLOBAL_TAGS_ASPECT_NAME)) {
      return new GlobalTags(
          response.getAspects().get(Constants.GLOBAL_TAGS_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }
}
