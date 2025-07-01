package com.linkedin.datahub.graphql.resolvers.recommendation;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ContentParams;
import com.linkedin.datahub.graphql.generated.EntityProfileParams;
import com.linkedin.datahub.graphql.generated.FacetFilter;
import com.linkedin.datahub.graphql.generated.ListRecommendationsInput;
import com.linkedin.datahub.graphql.generated.ListRecommendationsResult;
import com.linkedin.datahub.graphql.generated.RecommendationContent;
import com.linkedin.datahub.graphql.generated.RecommendationModule;
import com.linkedin.datahub.graphql.generated.RecommendationParams;
import com.linkedin.datahub.graphql.generated.RecommendationRenderType;
import com.linkedin.datahub.graphql.generated.RecommendationRequestContext;
import com.linkedin.datahub.graphql.generated.SearchParams;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.recommendation.EntityRequestContext;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.recommendation.SearchRequestContext;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ListRecommendationsResolver
    implements DataFetcher<CompletableFuture<ListRecommendationsResult>> {

  private static final ListRecommendationsResult EMPTY_RECOMMENDATIONS =
      new ListRecommendationsResult(Collections.emptyList());

  private final RecommendationsService _recommendationsService;
  private final ViewService _viewService;

  @WithSpan
  @Override
  public CompletableFuture<ListRecommendationsResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final ListRecommendationsInput input =
        bindArgument(environment.getArgument("input"), ListRecommendationsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            log.debug("Listing recommendations for input {}", input);
            List<com.linkedin.metadata.recommendation.RecommendationModule> modules =
                _recommendationsService.listRecommendations(
                    context.getOperationContext(),
                    mapRequestContext(context.getOperationContext(), input.getRequestContext()),
                    viewFilter(context.getOperationContext(), _viewService, input.getViewUrn()),
                    input.getLimit());
            return ListRecommendationsResult.builder()
                .setModules(
                    modules.stream()
                        .map(m -> mapRecommendationModule(context, m))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList()))
                .build();
          } catch (Exception e) {
            log.error("Failed to get recommendations for input {}", input, e);
            return EMPTY_RECOMMENDATIONS;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private com.linkedin.metadata.recommendation.RecommendationRequestContext mapRequestContext(
      @Nonnull OperationContext opContext, RecommendationRequestContext requestContext) {
    com.linkedin.metadata.recommendation.ScenarioType mappedScenarioType;
    try {
      mappedScenarioType =
          com.linkedin.metadata.recommendation.ScenarioType.valueOf(
              requestContext.getScenario().toString());
    } catch (IllegalArgumentException e) {
      log.error("Failed to map scenario type: {}", requestContext.getScenario(), e);
      throw e;
    }
    com.linkedin.metadata.recommendation.RecommendationRequestContext mappedRequestContext =
        new com.linkedin.metadata.recommendation.RecommendationRequestContext()
            .setScenario(mappedScenarioType);
    if (requestContext.getSearchRequestContext() != null) {
      SearchRequestContext searchRequestContext =
          new SearchRequestContext().setQuery(requestContext.getSearchRequestContext().getQuery());
      if (requestContext.getSearchRequestContext().getFilters() != null) {
        searchRequestContext.setFilters(
            new CriterionArray(
                requestContext.getSearchRequestContext().getFilters().stream()
                    .map(ResolverUtils::criterionFromFilter)
                    .collect(Collectors.toList())));
      }
      mappedRequestContext.setSearchRequestContext(searchRequestContext);
    }
    if (requestContext.getEntityRequestContext() != null) {
      Urn entityUrn;
      try {
        entityUrn = Urn.createFromString(requestContext.getEntityRequestContext().getUrn());
      } catch (URISyntaxException e) {
        log.error(
            "Malformed URN while mapping recommendations request: {}",
            requestContext.getEntityRequestContext().getUrn(),
            e);
        throw new IllegalArgumentException(e);
      }
      EntityRequestContext entityRequestContext =
          new EntityRequestContext()
              .setUrn(entityUrn)
              .setType(
                  EntityTypeMapper.getName(requestContext.getEntityRequestContext().getType()));
      mappedRequestContext.setEntityRequestContext(entityRequestContext);
    }
    return mappedRequestContext;
  }

  private Optional<RecommendationModule> mapRecommendationModule(
      @Nullable QueryContext context,
      com.linkedin.metadata.recommendation.RecommendationModule module) {
    RecommendationModule mappedModule = new RecommendationModule();
    mappedModule.setTitle(module.getTitle());
    mappedModule.setModuleId(module.getModuleId());
    try {
      mappedModule.setRenderType(
          RecommendationRenderType.valueOf(module.getRenderType().toString()));
    } catch (IllegalArgumentException e) {
      log.error("Failed to map render type: {}", module.getRenderType(), e);
      throw e;
    }
    mappedModule.setContent(
        module.getContent().stream()
            .map(c -> mapRecommendationContent(context, c))
            .collect(Collectors.toList()));
    return Optional.of(mappedModule);
  }

  private RecommendationContent mapRecommendationContent(
      @Nullable QueryContext context,
      com.linkedin.metadata.recommendation.RecommendationContent content) {
    RecommendationContent mappedContent = new RecommendationContent();
    mappedContent.setValue(content.getValue());
    if (content.hasEntity()) {
      mappedContent.setEntity(UrnToEntityMapper.map(context, content.getEntity()));
    }
    if (content.hasParams()) {
      mappedContent.setParams(mapRecommendationParams(content.getParams()));
    }
    return mappedContent;
  }

  private RecommendationParams mapRecommendationParams(
      com.linkedin.metadata.recommendation.RecommendationParams params) {
    RecommendationParams mappedParams = new RecommendationParams();
    if (params.hasSearchParams()) {
      SearchParams searchParams = new SearchParams();
      searchParams.setQuery(params.getSearchParams().getQuery());
      if (!params.getSearchParams().getFilters().isEmpty()) {
        searchParams.setFilters(
            params.getSearchParams().getFilters().stream()
                .map(
                    criterion ->
                        FacetFilter.builder()
                            .setField(criterion.getField())
                            .setValues(criterion.getValues())
                            .build())
                .collect(Collectors.toList()));
      }
      mappedParams.setSearchParams(searchParams);
    }

    if (params.hasEntityProfileParams()) {
      Urn profileUrn = params.getEntityProfileParams().getUrn();
      mappedParams.setEntityProfileParams(
          EntityProfileParams.builder()
              .setUrn(profileUrn.toString())
              .setType(EntityTypeMapper.getType(profileUrn.getEntityType()))
              .build());
    }

    if (params.hasContentParams()) {
      mappedParams.setContentParams(
          ContentParams.builder().setCount(params.getContentParams().getCount()).build());
    }

    return mappedParams;
  }
}
