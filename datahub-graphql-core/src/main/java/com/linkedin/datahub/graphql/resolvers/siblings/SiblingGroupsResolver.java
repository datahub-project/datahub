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
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Resolves {@code siblingGroups}: for each requested dataset, its sibling group's size and a page
 * of its other members, primary first. Used by search to show a result's siblings under it without
 * changing the search query.
 */
public class SiblingGroupsResolver implements DataFetcher<CompletableFuture<List<SiblingGroup>>> {

  static final int MAX_URNS = 50;
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

  @Override
  public CompletableFuture<List<SiblingGroup>> get(final DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final SiblingGroupsInput input =
        bindArgument(environment.getArgument("input"), SiblingGroupsInput.class);
    final List<String> requested = new ArrayList<>(new LinkedHashSet<>(input.getUrns()));
    if (requested.size() > MAX_URNS) {
      throw new IllegalArgumentException(
          String.format(
              "siblingGroups accepts at most %d urns, got %d", MAX_URNS, requested.size()));
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
    final int count =
        Math.min(
            MAX_COUNT, Math.max(0, input.getCount() != null ? input.getCount() : DEFAULT_COUNT));

    return GraphQLConcurrencyUtils.supplyAsync(
        () ->
            urns.stream()
                .map(urn -> resolveGroup(context, urn, start, count))
                .collect(Collectors.toList()),
        this.getClass().getSimpleName(),
        "get");
  }

  @Nonnull
  private SiblingGroup resolveGroup(
      @Nonnull final QueryContext context,
      @Nonnull final Urn urn,
      final int start,
      final int count) {
    final OperationContext opContext = context.getOperationContext();
    final SiblingGroup group = new SiblingGroup();
    group.setUrn(urn.toString());
    // A dataset the caller can't view gets an empty group, so its siblings and their count don't
    // reveal anything about it.
    if (!_canView.test(opContext, urn)) {
      group.setTotal(0);
      group.setTruncated(false);
      group.setMembers(List.of());
      return group;
    }
    final SiblingGroupMembers groupMembers =
        _siblingGraphService.siblingGroupMembers(opContext, urn);
    final List<SiblingGroupMember> visible =
        groupMembers.members().stream()
            .filter(member -> _canView.test(opContext, member.urn()))
            .collect(Collectors.toList());

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
