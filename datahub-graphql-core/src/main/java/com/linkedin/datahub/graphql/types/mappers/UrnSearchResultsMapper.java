package com.linkedin.datahub.graphql.types.mappers;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;
import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyInfoPolicyMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.KnownEntities;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.utils.UnknownDataGuard;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UrnSearchResultsMapper<T extends RecordTemplate, E extends Entity> {
  private static final UnknownDataGuard UNREPRESENTABLE =
      UnknownDataGuard.forSite(UrnSearchResultsMapper.class, "search hit");
  private static final UnknownDataGuard POLICY_CHECK =
      UnknownDataGuard.forSite(UrnSearchResultsMapper.class, "the policy condition check for");

  public static <T extends RecordTemplate, E extends Entity> SearchResults map(
      @Nullable final QueryContext context,
      com.linkedin.metadata.search.SearchResult searchResult) {
    return new UrnSearchResultsMapper<T, E>().apply(context, searchResult);
  }

  public SearchResults apply(
      @Nullable final QueryContext context, com.linkedin.metadata.search.SearchResult input) {
    final SearchResults result = new SearchResults();

    if (!input.hasFrom() || !input.hasPageSize() || !input.hasNumEntities()) {
      return result;
    }

    result.setStart(input.getFrom());
    result.setCount(input.getPageSize());
    result.setTotal(input.getNumEntities());

    final SearchResultMetadata searchResultMetadata = input.getMetadata();
    result.setSearchResults(mapKnownResults(context, input.getEntities()));
    result.setFacets(
        searchResultMetadata.getAggregations().stream()
            .map(f -> MapperUtils.mapFacet(context, f))
            .collect(Collectors.toList()));
    result.setSuggestions(
        searchResultMetadata.getSuggestions().stream()
            .map(MapperUtils::mapSearchSuggestion)
            .collect(Collectors.toList()));
    if (searchResultMetadata.hasRouteChain()) {
      result.setRouteChain(new ArrayList<>(searchResultMetadata.getRouteChain()));
    }

    return result;
  }

  /**
   * Maps search hits to results. GraphQL declares SearchResult.entity as non-null, and a single
   * null element bubbles up through the non-null [SearchResult!]! list and fails the entire query.
   * So hits GraphQL can't represent are left out, one row each, instead: entity types it doesn't
   * model, and entity types the registry doesn't know, including inside the urn's key (e.g. a
   * monitor on an entity type a newer version added, read after a rollback), and schema fields
   * whose parent GraphQL can't map (their non-null SchemaFieldEntity.parent would be unset). While
   * unknown enum values are tolerated, policies with a match condition this version doesn't know
   * are left out too, as listPolicies does (their non-null PolicyMatchCriterion.condition would be
   * unset); that reads the page's policy hits' info once. Totals stay the backend hit count, as
   * with authorization post-filtering. When unknown data isn't tolerated, only hits GraphQL doesn't
   * model are left out, without a registry check or a read.
   */
  static List<SearchResult> mapKnownResults(
      @Nullable final QueryContext context, @Nonnull final List<SearchEntity> hits) {
    // Checked under its own flag: the policy loader returns these policies as missing whenever
    // unknown enum values are tolerated, whether or not unknown entities are.
    final Set<Urn> unrepresentablePolicies = unrepresentablePolicies(context, hits);
    if (!handling().toleratesUnknownEntityAspect()) {
      final List<SearchResult> mappedResults = new ArrayList<>();
      for (SearchEntity searchEntity : hits) {
        final SearchResult mapped = MapperUtils.mapResult(context, searchEntity);
        if (mapped.getEntity() == null) {
          log.warn(
              "Search hit {} has no GraphQL entity mapping; excluding it from results rather than "
                  + "failing the whole result set.",
              searchEntity.getEntity());
          continue;
        }
        if (unrepresentablePolicies.contains(searchEntity.getEntity())) {
          continue;
        }
        mappedResults.add(mapped);
      }
      return mappedResults;
    }
    final List<SearchResult> mappedResults = new ArrayList<>(hits.size());
    for (SearchEntity hit : hits) {
      final boolean known = KnownEntities.isKnown(context, hit.getEntity());
      final SearchResult mapped = known ? MapperUtils.mapResult(context, hit) : null;
      if (mapped == null || mapped.getEntity() == null) {
        UNREPRESENTABLE.skippedBecause(
            KnownEntities.skipMetrics(context, known),
            hit.getEntity().getEntityType(),
            "GraphQL can't represent its entity type",
            hit.getEntity());
        continue;
      }
      if (!KnownEntities.hasRepresentableParent(context, hit.getEntity())) {
        continue;
      }
      if (unrepresentablePolicies.contains(hit.getEntity())) {
        continue;
      }
      mappedResults.add(mapped);
    }
    return mappedResults;
  }

  /**
   * Policy hits GraphQL can't show, by the rule listPolicies uses: a match condition this version
   * doesn't know. The policy loader returns them as missing, and a missing entity in the non-null
   * SearchResult.entity would null the whole page, so they are left out first. One aspect read per
   * page holding policy hits, only while unknown enum values are tolerated.
   */
  @Nonnull
  static Set<Urn> unrepresentablePolicies(
      @Nullable final QueryContext context, @Nonnull final List<SearchEntity> hits) {
    if (!handling().toleratesUnknownEnum()) {
      return Set.of();
    }
    final Set<Urn> policies =
        hits.stream()
            .map(SearchEntity::getEntity)
            .filter(urn -> POLICY_ENTITY_NAME.equals(urn.getEntityType()))
            .collect(Collectors.toSet());
    final OperationContext opContext = context == null ? null : context.getOperationContext();
    if (policies.isEmpty() || opContext == null) {
      return Set.of();
    }
    final Map<Urn, Map<String, Aspect>> infos;
    try {
      infos =
          opContext
              .getAspectRetriever()
              .getLatestAspectObjects(opContext, policies, Set.of(DATAHUB_POLICY_INFO_ASPECT_NAME));
    } catch (RuntimeException e) {
      // Without the read none of them can be checked, and the loader would still return the
      // unrepresentable ones as missing, nulling the page: the page's policy hits are left out.
      POLICY_CHECK.skippedBecause(
          Optional.empty(),
          "dataHubPolicyInfo read",
          "could not read dataHubPolicyInfo, so the page's policy hits are left out: " + e,
          policies.size() + " policy hits");
      log.debug("dataHubPolicyInfo read failure", e);
      return policies;
    }
    final Set<Urn> unrepresentable = new HashSet<>();
    for (Map.Entry<Urn, Map<String, Aspect>> entry : infos.entrySet()) {
      final Aspect aspect = entry.getValue().get(DATAHUB_POLICY_INFO_ASPECT_NAME);
      if (aspect != null
          && PolicyInfoPolicyMapper.isUnrepresentable(
              new DataHubPolicyInfo(aspect.data()), entry.getKey())) {
        unrepresentable.add(entry.getKey());
      }
    }
    return unrepresentable;
  }
}
