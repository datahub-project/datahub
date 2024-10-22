package com.linkedin.metadata.search.elasticsearch.query.filter;

import static com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType.AUTOCOMPLETE;
import static com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType.FULLTEXT_SEARCH;
import static com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType.STRUCTURED_SEARCH;

import com.linkedin.metadata.config.search.QueryFilterRewriterConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.index.query.QueryBuilder;

@Builder
public class DomainExpansionRewriter extends BaseQueryFilterRewriter {

  @Getter
  private final Set<QueryFilterRewriterSearchType> rewriterSearchTypes =
      Set.of(AUTOCOMPLETE, FULLTEXT_SEARCH, STRUCTURED_SEARCH);

  @Builder.Default private Condition defaultCondition = Condition.DESCENDANTS_INCL;

  @Nonnull private final QueryFilterRewriterConfiguration.ExpansionRewriterConfiguration config;

  @Nonnull
  @Override
  public Set<String> getRewriterFieldNames() {
    return Set.of("domains.keyword");
  }

  @Override
  public <T extends QueryBuilder> T rewrite(
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriterContext rewriterContext,
      @Nullable T filterQuery) {

    if (filterQuery != null && isQueryTimeEnabled(rewriterContext)) {
      switch (rewriterContext.getCondition() == null
          ? defaultCondition
          : rewriterContext.getCondition()) {
        case DESCENDANTS_INCL:
          return expandUrnsByGraph(
              opContext,
              filterQuery,
              List.of("IsPartOf"),
              RelationshipDirection.INCOMING,
              config.getPageSize(),
              config.getLimit());
        case ANCESTORS_INCL:
          return expandUrnsByGraph(
              opContext,
              filterQuery,
              List.of("IsPartOf"),
              RelationshipDirection.OUTGOING,
              config.getPageSize(),
              config.getLimit());
        default:
          // UNDIRECTED doesn't work at the graph service layer
          // RelationshipDirection.UNDIRECTED;
          T descendantQuery =
              expandUrnsByGraph(
                  opContext,
                  filterQuery,
                  List.of("IsPartOf"),
                  RelationshipDirection.INCOMING,
                  config.getPageSize(),
                  config.getLimit());
          return expandUrnsByGraph(
              opContext,
              descendantQuery,
              List.of("IsPartOf"),
              RelationshipDirection.OUTGOING,
              config.getPageSize(),
              config.getLimit());
      }
    }

    return filterQuery;
  }
}
