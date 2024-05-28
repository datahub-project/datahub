package com.linkedin.datahub.graphql.resolvers.businessattribute;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListBusinessAttributesInput;
import com.linkedin.datahub.graphql.generated.ListBusinessAttributesResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver used for listing Business Attributes. */
@Slf4j
public class ListBusinessAttributesResolver
    implements DataFetcher<CompletableFuture<ListBusinessAttributesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  public ListBusinessAttributesResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<ListBusinessAttributesResult> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final ListBusinessAttributesInput input =
        bindArgument(environment.getArgument("input"), ListBusinessAttributesInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
          final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
          final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();

          try {

            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME,
                    query,
                    Collections.emptyMap(),
                    start,
                    count);

            final ListBusinessAttributesResult result = new ListBusinessAttributesResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setBusinessAttributes(
                mapUnresolvedBusinessAttributes(
                    gmsResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList())));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list Business Attributes", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private List<BusinessAttribute> mapUnresolvedBusinessAttributes(final List<Urn> entityUrns) {
    final List<BusinessAttribute> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final BusinessAttribute unresolvedBusinessAttribute = new BusinessAttribute();
      unresolvedBusinessAttribute.setUrn(urn.toString());
      unresolvedBusinessAttribute.setType(EntityType.BUSINESS_ATTRIBUTE);
      results.add(unresolvedBusinessAttribute);
    }
    return results;
  }
}
