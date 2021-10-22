package com.linkedin.datahub.graphql.resolvers.recommendation;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ContentParams;
import com.linkedin.datahub.graphql.generated.EntityProfileParams;
import com.linkedin.datahub.graphql.generated.Filter;
import com.linkedin.datahub.graphql.generated.ListRecommendationsInput;
import com.linkedin.datahub.graphql.generated.ListRecommendationsResult;
import com.linkedin.datahub.graphql.generated.RecommendationContent;
import com.linkedin.datahub.graphql.generated.RecommendationModule;
import com.linkedin.datahub.graphql.generated.RecommendationParams;
import com.linkedin.datahub.graphql.generated.RecommendationRenderType;
import com.linkedin.datahub.graphql.generated.RecommendationRequestContext;
import com.linkedin.datahub.graphql.generated.SearchParams;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.recommendation.EntityRequestContext;
import com.linkedin.metadata.recommendation.RecommendationService;
import com.linkedin.metadata.recommendation.SearchRequestContext;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


@Slf4j
@RequiredArgsConstructor
public class ListRecommendationsResolver implements DataFetcher<CompletableFuture<ListRecommendationsResult>> {
  private final RecommendationService _recommendationService;

  @Override
  public CompletableFuture<ListRecommendationsResult> get(DataFetchingEnvironment environment) {
    final ListRecommendationsInput input =
        bindArgument(environment.getArgument("input"), ListRecommendationsInput.class);

    return CompletableFuture.supplyAsync(() -> {
      try {
        log.debug("Listing recommendations for input {}", input);
        List<com.linkedin.metadata.recommendation.RecommendationModule> modules =
            _recommendationService.listRecommendations(Urn.createFromString(input.getUserUrn()),
                mapRequestContext(input.getRequestContext()), input.getLimit());
        return ListRecommendationsResult.builder()
            .setModules(modules.stream()
                .map(this::mapRecommendationModule)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()))
            .build();
      } catch (Exception e) {
        log.error("Failed to get recommendations for input {}", input);
        throw new RuntimeException("Failed to get recommendations for input " + input, e);
      }
    });
  }

  private com.linkedin.metadata.recommendation.RecommendationRequestContext mapRequestContext(
      RecommendationRequestContext requestContext) {
    com.linkedin.metadata.recommendation.ScenarioType mappedScenarioType;
    try {
      mappedScenarioType =
          com.linkedin.metadata.recommendation.ScenarioType.valueOf(requestContext.getScenario().toString());
    } catch (IllegalArgumentException e) {
      log.error("Failed to map scenario type: {}", requestContext.getScenario(), e);
      throw e;
    }
    com.linkedin.metadata.recommendation.RecommendationRequestContext mappedRequestContext =
        new com.linkedin.metadata.recommendation.RecommendationRequestContext().setScenario(mappedScenarioType);
    if (requestContext.getSearchRequestContext() != null) {
      SearchRequestContext searchRequestContext =
          new SearchRequestContext().setQuery(requestContext.getSearchRequestContext().getQuery());
      if (requestContext.getSearchRequestContext().getFilters() != null) {
        searchRequestContext.setFilters(new CriterionArray(requestContext.getSearchRequestContext()
            .getFilters()
            .stream()
            .map(facetField -> new Criterion().setField(facetField.getField()).setValue(facetField.getValue()))
            .collect(Collectors.toList())));
      }
      mappedRequestContext.setSearchRequestContext(searchRequestContext);
    }
    if (requestContext.getEntityRequestContext() != null) {
      Urn entityUrn;
      try {
        entityUrn = Urn.createFromString(requestContext.getEntityRequestContext().getUrn());
      } catch (URISyntaxException e) {
        log.error("Malformed URN while mapping recommendations request: {}",
            requestContext.getEntityRequestContext().getUrn(), e);
        throw new IllegalArgumentException(e);
      }
      EntityRequestContext entityRequestContext = new EntityRequestContext().setUrn(entityUrn)
          .setType(EntityTypeMapper.getName(requestContext.getEntityRequestContext().getType()));
      mappedRequestContext.setEntityRequestContext(entityRequestContext);
    }
    return mappedRequestContext;
  }

  private Optional<RecommendationModule> mapRecommendationModule(
      com.linkedin.metadata.recommendation.RecommendationModule module) {
    RecommendationModule mappedModule = new RecommendationModule();
    mappedModule.setTitle(module.getTitle());
    mappedModule.setModuleId(module.getModuleId());
    try {
      mappedModule.setRenderType(RecommendationRenderType.valueOf(module.getRenderType().toString()));
    } catch (IllegalArgumentException e) {
      log.error("Failed to map render type: {}", module.getRenderType(), e);
      throw e;
    }
    mappedModule.setContent(
        module.getContent().stream().map(this::mapRecommendationContent).collect(Collectors.toList()));
    return Optional.of(mappedModule);
  }

  private RecommendationContent mapRecommendationContent(
      com.linkedin.metadata.recommendation.RecommendationContent content) {
    RecommendationContent mappedContent = new RecommendationContent();
    mappedContent.setValue(content.getValue());
    if (content.hasEntity()) {
      mappedContent.setEntity(UrnToEntityMapper.map(content.getEntity()));
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
        searchParams.setFilters(params.getSearchParams()
            .getFilters()
            .stream()
            .map(criterion -> Filter.builder().setField(criterion.getField()).setValue(criterion.getValue()).build())
            .collect(Collectors.toList()));
      }
      mappedParams.setSearchParams(searchParams);
    }

    if (params.hasEntityProfileParams()) {
      mappedParams.setEntityProfileParams(
          EntityProfileParams.builder().setUrn(params.getEntityProfileParams().getUrn().toString()).build());
    }

    if (params.hasContentParams()) {
      mappedParams.setContentParams(ContentParams.builder().setCount(params.getContentParams().getCount()).build());
    }

    return mappedParams;
  }

  private ListRecommendationsResult createMockSearchResults() {
    // Popular Tags
    List<String> popularTagList =
        ImmutableList.of("urn:li:tag:Legacy", "urn:li:tag:Email", "urn:li:tag:PII", "urn:li:tag:Highly Confidential",
            "urn:li:tag:Marketing", "urn:li:tag:SalesDev", "urn:li:tag:MemberRefreshQ3");
    RecommendationModule popularTags = new RecommendationModule();
    popularTags.setTitle("Popular Tags");
    popularTags.setModuleId("PopularTags");
    popularTags.setRenderType(RecommendationRenderType.TAG_SEARCH_LIST);
    popularTags.setContent(popularTagList.stream().map(this::createTagSearchRec).collect(Collectors.toList()));

    return ListRecommendationsResult.builder().setModules(ImmutableList.of(popularTags)).build();
  }

  private ListRecommendationsResult createMockEntityResults() {
    // People also viewed
    List<String> peopleAlsoViewed = ImmutableList.of("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)", "urn:li:dashboard:(looker,baz)",
        "urn:li:glossaryTerm:AccountBalance");
    RecommendationModule peopleAlsoViewedModule = new RecommendationModule();
    peopleAlsoViewedModule.setTitle("People also viewed");
    peopleAlsoViewedModule.setModuleId("PeopleAlsoViewed");
    peopleAlsoViewedModule.setRenderType(RecommendationRenderType.ENTITY_NAME_LIST);
    peopleAlsoViewedModule.setContent(
        peopleAlsoViewed.stream().map(this::createEntityRec).collect(Collectors.toList()));

    return ListRecommendationsResult.builder().setModules(ImmutableList.of(peopleAlsoViewedModule)).build();
  }

  private ListRecommendationsResult createMockHomeResults() {
    // Top platforms
    List<String> platforms =
        ImmutableList.of("urn:li:dataPlatform:bigquery", "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:looker",
            "urn:li:dataPlatform:airflow");
    RecommendationModule topPlatformModule = new RecommendationModule();
    topPlatformModule.setTitle("Top Platforms");
    topPlatformModule.setModuleId("TopPlatforms");
    topPlatformModule.setRenderType(RecommendationRenderType.PLATFORM_SEARCH_LIST);
    topPlatformModule.setContent(platforms.stream().map(this::createPlatformRec).collect(Collectors.toList()));

    // Recently Viewed
    List<String> recentlyViewed = ImmutableList.of("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)", "urn:li:dashboard:(looker,baz)",
        "urn:li:corpuser:datahub", "urn:li:corpGroup:bfoo");
    RecommendationModule recentlyViewedModule = new RecommendationModule();
    recentlyViewedModule.setTitle("Recently Viewed");
    recentlyViewedModule.setModuleId("RecentlyViewed");
    recentlyViewedModule.setRenderType(RecommendationRenderType.ENTITY_NAME_LIST);
    recentlyViewedModule.setContent(recentlyViewed.stream().map(this::createEntityRec).collect(Collectors.toList()));

    // Popular entities
    List<String> popularEntities = ImmutableList.of("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,logging_events,PROD)", "urn:li:dataFlow:(airflow,dag_abc,PROD)",
        "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,user_analytics)");
    RecommendationModule popularEntitiesModule = new RecommendationModule();
    popularEntitiesModule.setTitle("Most Popular");
    popularEntitiesModule.setModuleId("PopularEntities");
    popularEntitiesModule.setRenderType(RecommendationRenderType.ENTITY_NAME_LIST);
    popularEntitiesModule.setContent(popularEntities.stream().map(this::createEntityRec).collect(Collectors.toList()));

    return ListRecommendationsResult.builder()
        .setModules(ImmutableList.of(topPlatformModule, recentlyViewedModule, popularEntitiesModule))
        .build();
  }

  private RecommendationContent createPlatformRec(String platformUrn) {
    RecommendationContent content = new RecommendationContent();
    content.setValue(platformUrn);
    try {
      content.setEntity(UrnToEntityMapper.map(Urn.createFromString(platformUrn)));
    } catch (URISyntaxException e) {
      log.error("Invalid urn: {}", platformUrn);
    }
    content.setParams(RecommendationParams.builder()
        .setSearchParams(SearchParams.builder()
            .setQuery("")
            .setFilters(ImmutableList.of(Filter.builder().setField("platform").setValue(platformUrn).build()))
            .build())
        .build());
    return content;
  }

  private RecommendationContent createEntityRec(String entityUrn) {
    RecommendationContent content = new RecommendationContent();
    content.setValue(entityUrn);
    try {
      content.setEntity(UrnToEntityMapper.map(Urn.createFromString(entityUrn)));
    } catch (URISyntaxException e) {
      log.error("Invalid urn: {}", entityUrn);
    }
    content.setParams(RecommendationParams.builder()
        .setEntityProfileParams(EntityProfileParams.builder().setUrn(entityUrn).build())
        .build());
    return content;
  }

  private RecommendationContent createTagSearchRec(String tagUrn) {
    RecommendationContent content = new RecommendationContent();
    content.setValue(tagUrn);
    try {
      content.setEntity(UrnToEntityMapper.map(Urn.createFromString(tagUrn)));
    } catch (URISyntaxException e) {
      log.error("Invalid urn: {}", tagUrn);
    }
    content.setParams(RecommendationParams.builder()
        .setSearchParams(SearchParams.builder()
            .setQuery("")
            .setFilters(ImmutableList.of(Filter.builder().setField("tags").setValue(tagUrn).build()))
            .build())
        .build());
    return content;
  }
}
