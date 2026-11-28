package com.linkedin.datahub.graphql.resolvers.customfabrictype;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CustomFabricType;
import com.linkedin.datahub.graphql.generated.ListCustomFabricTypesInput;
import com.linkedin.datahub.graphql.generated.ListCustomFabricTypesResult;
import com.linkedin.datahub.graphql.types.customfabrictype.CustomFabricTypeMapper;
import com.linkedin.datahub.graphql.types.customfabrictype.CustomFabricTypeType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.ListUrnsResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * The registered custom environments, read from primary storage so a new one is listed as soon as
 * it is created. Environments are few, so all of them are read, then filtered (soft-deleted ones
 * are always left out, deprecated ones on request) and paged.
 */
@RequiredArgsConstructor
public class ListCustomFabricTypesResolver
    implements DataFetcher<CompletableFuture<ListCustomFabricTypesResult>> {

  static final int DEFAULT_COUNT = 100;
  private static final int LIST_PAGE_SIZE = 1000;

  @Nonnull private final EntityClient entityClient;

  @Override
  public CompletableFuture<ListCustomFabricTypesResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final ListCustomFabricTypesInput input =
        bindArgument(environment.getArgument("input"), ListCustomFabricTypesInput.class);
    final int start = input.getStart() == null ? 0 : input.getStart();
    final int count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final boolean includeDeprecated = !Boolean.FALSE.equals(input.getIncludeDeprecated());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            List<Urn> urns = new ArrayList<>();
            int offset = 0;
            ListUrnsResult page;
            do {
              page =
                  entityClient.listUrns(
                      context.getOperationContext(),
                      CUSTOM_FABRIC_TYPE_ENTITY_NAME,
                      offset,
                      LIST_PAGE_SIZE);
              if (page == null || page.getEntities().isEmpty()) {
                break;
              }
              urns.addAll(page.getEntities());
              offset += page.getEntities().size();
            } while (offset < page.getTotal());

            Map<Urn, EntityResponse> responses =
                urns.isEmpty()
                    ? Map.of()
                    : entityClient.batchGetV2(
                        context.getOperationContext(),
                        CUSTOM_FABRIC_TYPE_ENTITY_NAME,
                        new LinkedHashSet<>(urns),
                        CustomFabricTypeType.ASPECTS_TO_FETCH);
            List<CustomFabricType> matching =
                urns.stream()
                    .map(responses::get)
                    .filter(Objects::nonNull)
                    .map(response -> CustomFabricTypeMapper.map(context, response))
                    .filter(
                        type ->
                            type.getStatus() == null
                                || !Boolean.TRUE.equals(type.getStatus().getRemoved()))
                    .filter(
                        type ->
                            includeDeprecated
                                || type.getDeprecation() == null
                                || !Boolean.TRUE.equals(type.getDeprecation().getDeprecated()))
                    .sorted(Comparator.comparing(CustomFabricType::getId))
                    .collect(Collectors.toList());

            ListCustomFabricTypesResult result = new ListCustomFabricTypesResult();
            List<CustomFabricType> pageItems =
                matching.stream().skip(start).limit(count).collect(Collectors.toList());
            result.setStart(start);
            result.setCount(pageItems.size());
            result.setTotal(matching.size());
            result.setCustomFabricTypes(pageItems);
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list environments", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
