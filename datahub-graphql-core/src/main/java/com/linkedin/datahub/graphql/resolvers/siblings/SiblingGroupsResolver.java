package com.linkedin.datahub.graphql.resolvers.siblings;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.SiblingGroup;
import com.linkedin.datahub.graphql.generated.SiblingGroupsInput;
import com.linkedin.datahub.graphql.generated.SiblingMember;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.graph.SiblingGraphService.SiblingGroupMember;
import com.linkedin.metadata.graph.SiblingGraphService.SiblingGroupMembers;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Resolves {@code siblingGroups}: for each requested dataset, its sibling group's size and a page
 * of its other members, primary first. Used by search to show a result's siblings under it without
 * changing the search query.
 */
public class SiblingGroupsResolver implements DataFetcher<CompletableFuture<List<SiblingGroup>>> {

  static final int DEFAULT_COUNT = 5;
  static final int MAX_COUNT = 200;

  private final SiblingGraphService _siblingGraphService;
  // Per-urn view authorization; injected for tests, since the resolver runs off the calling thread.
  private final BiPredicate<OperationContext, Urn> _canView;

  public SiblingGroupsResolver(@Nonnull final SiblingGraphService siblingGraphService) {
    this(siblingGraphService, AuthorizationUtils::canView);
  }

  SiblingGroupsResolver(
      @Nonnull final SiblingGraphService siblingGraphService,
      @Nonnull final BiPredicate<OperationContext, Urn> canView) {
    _siblingGraphService = siblingGraphService;
    _canView = canView;
  }

  /** The page size used for a requested {@code count}; QueryCostInstrumentation prices by it. */
  public static int pageSize(@Nullable final Integer count) {
    return Math.min(MAX_COUNT, Math.max(0, count != null ? count : DEFAULT_COUNT));
  }

  @Override
  public CompletableFuture<List<SiblingGroup>> get(final DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final SiblingGroupsInput input =
        bindArgument(environment.getArgument("input"), SiblingGroupsInput.class);
    final List<String> requested = new ArrayList<>(new LinkedHashSet<>(input.getUrns()));
    final int maxUrns = context.getMaxSiblingGroupUrns();
    if (maxUrns <= 0) {
      throw new IllegalArgumentException(
          "siblingGroups is off. Set GRAPHQL_QUERY_MAX_SIBLING_GROUP_URNS above 0 to enable it.");
    }
    if (requested.size() > maxUrns) {
      throw new IllegalArgumentException(
          String.format(
              "siblingGroups accepts at most %d urns, got %d", maxUrns, requested.size()));
    }
    // Validate before going async, so a bad urn is a request error rather than a server error.
    final List<Urn> urns = new ArrayList<>(requested.size());
    for (String urn : requested) {
      final Urn parsed;
      try {
        parsed = Urn.createFromString(urn);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Invalid urn: " + urn, e);
      }
      if (!DATASET_ENTITY_NAME.equals(parsed.getEntityType())) {
        throw new IllegalArgumentException("siblingGroups only accepts dataset urns: " + urn);
      }
      urns.add(parsed);
    }
    final int start = Math.max(0, input.getStart() != null ? input.getStart() : 0);
    final int count = pageSize(input.getCount());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> resolveGroups(context, urns, start, count), this.getClass().getSimpleName(), "get");
  }

  /** Resolves every requested group with one call to the sibling graph. */
  @Nonnull
  private List<SiblingGroup> resolveGroups(
      @Nonnull final QueryContext context,
      @Nonnull final List<Urn> urns,
      final int start,
      final int count) {
    final OperationContext opContext = context.getOperationContext();
    final Predicate<Urn> canView = urn -> _canView.test(opContext, urn);
    // Only datasets the caller can view are walked from, and a hidden one gets an empty group, so
    // its siblings and their count reveal nothing about it. A hidden dataset can still be read as
    // a member of a visible dataset's group, but it is then filtered out of that group's page.
    final List<Urn> walked = urns.stream().filter(canView).collect(Collectors.toList());
    final Map<Urn, SiblingGroupMembers> groups =
        walked.isEmpty() ? Map.of() : _siblingGraphService.siblingGroupMembers(opContext, walked);
    final SiblingGroupMembers hidden = new SiblingGroupMembers(List.of(), false);
    return urns.stream()
        .map(urn -> toGroup(context, urn, groups.getOrDefault(urn, hidden), canView, start, count))
        .collect(Collectors.toList());
  }

  @Nonnull
  private static SiblingGroup toGroup(
      @Nonnull final QueryContext context,
      @Nonnull final Urn urn,
      @Nonnull final SiblingGroupMembers groupMembers,
      @Nonnull final Predicate<Urn> canView,
      final int start,
      final int count) {
    final List<SiblingGroupMember> visible =
        groupMembers.members().stream()
            .filter(member -> canView.test(member.urn()))
            .collect(Collectors.toList());
    final SiblingGroup group = new SiblingGroup();
    group.setUrn(urn.toString());
    group.setTotal(visible.size());
    group.setTruncated(groupMembers.truncated());
    group.setMembers(
        visible.stream()
            .skip(start)
            .limit(count)
            .map(
                member -> {
                  final SiblingMember result = new SiblingMember();
                  result.setEntity(UrnToEntityMapper.map(context, member.urn()));
                  result.setIsPrimary(member.primary());
                  return result;
                })
            .collect(Collectors.toList()));
    return group;
  }
}
