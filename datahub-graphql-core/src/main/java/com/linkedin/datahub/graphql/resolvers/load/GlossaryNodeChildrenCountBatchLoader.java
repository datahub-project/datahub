package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.metadata.utils.SearchUtil.AGGREGATION_SEPARATOR_CHAR;
import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.GlossaryNodeChildrenCount;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.CriterionUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * DataLoader for batching glossary node children counts. Instead of one aggregation query per
 * glossary node, a whole level of the glossary tree is resolved with a single {@code
 * parentNode␞_entityType} nested facet query filtered on every parent URN in the batch.
 */
@Slf4j
@RequiredArgsConstructor
public class GlossaryNodeChildrenCountBatchLoader {

  public static final String LOADER_NAME = "GlossaryNodeChildrenCount";

  private static final String PARENT_NODE_FIELD = "parentNode";
  private static final List<String> CHILD_ENTITY_NAMES =
      ImmutableList.of(Constants.GLOSSARY_TERM_ENTITY_NAME, Constants.GLOSSARY_NODE_ENTITY_NAME);
  private static final List<String> CHILDREN_BY_TYPE_FACET =
      ImmutableList.of(PARENT_NODE_FIELD + AGGREGATION_SEPARATOR_CHAR + INDEX_VIRTUAL_FIELD);

  /**
   * Parents resolved per aggregation query. The {@code parentNode} terms aggregation returns at
   * most {@code min(searchFlags.maxAggValues, elasticsearch.search.maxTermBucketSize)} buckets, and
   * {@code maxTermBucketSize} defaults to 60 — wider batches would drop parents from the response.
   * Kept below that ceiling so a moderately lowered cluster setting still leaves headroom; anything
   * that does slip through is caught by the truncation check in {@link #loadChunk}.
   */
  static final int MAX_PARENTS_PER_QUERY = 50;

  private final EntityClient entityClient;

  public static DataLoader<String, GlossaryNodeChildrenCount> createDataLoader(
      final EntityClient entityClient, final QueryContext queryContext) {
    final GlossaryNodeChildrenCountBatchLoader loader =
        new GlossaryNodeChildrenCountBatchLoader(entityClient);
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);
    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> loader.batchLoad(keys, (QueryContext) env.getContext()),
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  /**
   * Resolves the child term and node counts for each parent glossary node URN, in key order.
   * Parents without children are returned as zero counts rather than nulls.
   */
  public List<GlossaryNodeChildrenCount> batchLoad(
      final List<String> parentUrns, final QueryContext context) {
    final Map<String, GlossaryNodeChildrenCount> countsByParent = new HashMap<>();
    for (int i = 0; i < parentUrns.size(); i += MAX_PARENTS_PER_QUERY) {
      final List<String> chunk =
          parentUrns.subList(i, Math.min(i + MAX_PARENTS_PER_QUERY, parentUrns.size()));
      countsByParent.putAll(loadChunk(chunk, context));
    }

    return parentUrns.stream()
        .map(urn -> countsByParent.getOrDefault(urn, childrenCount(0, 0)))
        .collect(Collectors.toList());
  }

  private Map<String, GlossaryNodeChildrenCount> loadChunk(
      final List<String> parentUrns, final QueryContext context) {
    final SearchResult result = queryChildrenCounts(parentUrns, context);
    final Map<String, GlossaryNodeChildrenCount> counts = extractCounts(result);

    // Every matching child document lands in exactly one (parentNode, entity type) bucket, so the
    // aggregated counts must add up to the total hit count. Falling short means the parentNode
    // aggregation dropped buckets and the missing parents would report zero children, so redo the
    // chunk one parent at a time — slower, but a single-parent aggregation cannot be truncated.
    final long aggregatedTotal =
        counts.values().stream().mapToLong(c -> (long) c.getTermsCount() + c.getNodesCount()).sum();
    final long totalHits = result.hasNumEntities() ? result.getNumEntities() : 0L;
    if (parentUrns.size() > 1 && aggregatedTotal < totalHits) {
      log.warn(
          "Glossary children count aggregation truncated for {} parents ({} of {} children"
              + " attributed); falling back to per-parent queries. Consider raising"
              + " elasticsearch.search.maxTermBucketSize.",
          parentUrns.size(),
          aggregatedTotal,
          totalHits);
      final Map<String, GlossaryNodeChildrenCount> perParentCounts = new HashMap<>();
      for (final String parentUrn : parentUrns) {
        perParentCounts.putAll(
            extractCounts(queryChildrenCounts(ImmutableList.of(parentUrn), context)));
      }
      return perParentCounts;
    }

    return counts;
  }

  private SearchResult queryChildrenCounts(
      final List<String> parentUrns, final QueryContext context) {
    final Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                CriterionUtils.buildCriterion(
                                    PARENT_NODE_FIELD, Condition.EQUAL, parentUrns)))));
    try {
      return entityClient.searchAcrossEntities(
          // The default maxAggValues (20) is below our batch size, which would truncate the
          // parentNode buckets on every full batch.
          context
              .getOperationContext()
              .withSearchFlags(flags -> flags.setMaxAggValues(MAX_PARENTS_PER_QUERY)),
          CHILD_ENTITY_NAMES,
          "*",
          filter,
          0,
          0, // 0 entity count because only the aggregation is needed
          Collections.emptyList(),
          CHILDREN_BY_TYPE_FACET);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to fetch glossary children counts for parents %s", parentUrns), e);
    }
  }

  /**
   * Reads the nested facet values, which are {@code <parentNodeUrn>␞<entityName>} keys. The same
   * aggregation also carries single-token parent totals, which are skipped.
   */
  private static Map<String, GlossaryNodeChildrenCount> extractCounts(final SearchResult result) {
    final Map<String, GlossaryNodeChildrenCount> counts = new HashMap<>();
    if (!result.hasMetadata() || !result.getMetadata().hasAggregations()) {
      return counts;
    }

    for (final AggregationMetadata aggregation : result.getMetadata().getAggregations()) {
      if (!CHILDREN_BY_TYPE_FACET.get(0).equals(aggregation.getName())) {
        continue;
      }
      for (final FilterValue filterValue : aggregation.getFilterValues()) {
        final String[] tokens = filterValue.getValue().split(AGGREGATION_SEPARATOR_CHAR);
        if (tokens.length != 2) {
          continue;
        }
        final int count = filterValue.getFacetCount().intValue();
        final GlossaryNodeChildrenCount childrenCount =
            counts.computeIfAbsent(tokens[0], urn -> childrenCount(0, 0));
        // Facet values come back derived from the ES index name, so they are lower-cased
        // ("glossaryterm") rather than the camel-cased entity name.
        if (tokens[1].equalsIgnoreCase(Constants.GLOSSARY_TERM_ENTITY_NAME)) {
          childrenCount.setTermsCount(childrenCount.getTermsCount() + count);
        } else if (tokens[1].equalsIgnoreCase(Constants.GLOSSARY_NODE_ENTITY_NAME)) {
          childrenCount.setNodesCount(childrenCount.getNodesCount() + count);
        }
      }
    }
    return counts;
  }

  private static GlossaryNodeChildrenCount childrenCount(
      final int termsCount, final int nodesCount) {
    final GlossaryNodeChildrenCount count = new GlossaryNodeChildrenCount();
    count.setTermsCount(termsCount);
    count.setNodesCount(nodesCount);
    return count;
  }
}
