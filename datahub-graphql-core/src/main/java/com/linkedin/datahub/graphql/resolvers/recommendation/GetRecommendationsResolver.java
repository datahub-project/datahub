package com.linkedin.datahub.graphql.resolvers.recommendation;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityPageContext;
import com.linkedin.datahub.graphql.generated.Filter;
import com.linkedin.datahub.graphql.generated.GetRecommendationsInput;
import com.linkedin.datahub.graphql.generated.PageType;
import com.linkedin.datahub.graphql.generated.RecommendationContent;
import com.linkedin.datahub.graphql.generated.RecommendationModule;
import com.linkedin.datahub.graphql.generated.RecommendationResults;
import com.linkedin.datahub.graphql.generated.RenderType;
import com.linkedin.datahub.graphql.generated.SearchPageContext;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


@Slf4j
@RequiredArgsConstructor
public class GetRecommendationsResolver implements DataFetcher<CompletableFuture<RecommendationResults>> {

  @Override
  public CompletableFuture<RecommendationResults> get(DataFetchingEnvironment environment) {
    final GetRecommendationsInput input = bindArgument(environment.getArgument("input"), GetRecommendationsInput.class);

    return CompletableFuture.supplyAsync(() -> {
      try {
        log.debug("Getting recommendations for input {}", input);
        if (input.getRequestContext().getPageType() != PageType.HOME) {
          log.info("Recommendations are only supported on the home page");
          return new RecommendationResults(Collections.emptyList());
        }
        return createMockResults();
      } catch (Exception e) {
        log.error("Failed to get recommendations for input {}", input);
        throw new RuntimeException("Failed to get recommendations for input " + input, e);
      }
    });
  }

  private RecommendationResults createMockResults() {
    // Top platforms
    List<String> platforms =
        ImmutableList.of("urn:li:dataPlatform:bigquery", "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:looker",
            "urn:li:dataPlatform:airflow");
    RecommendationModule topPlatformModule = new RecommendationModule();
    topPlatformModule.setTitle("Top Platforms");
    topPlatformModule.setModuleType("TopPlatforms");
    topPlatformModule.setRenderType(RenderType.PLATFORM_LIST);
    topPlatformModule.setContent(platforms.stream().map(this::createPlatformRec).collect(Collectors.toList()));

    // Recently Viewed
    List<String> recentlyViewed = ImmutableList.of("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)", "urn:li:dashboard:(looker,baz)");
    RecommendationModule recentlyViewedModule = new RecommendationModule();
    recentlyViewedModule.setTitle("Recently Viewed");
    recentlyViewedModule.setModuleType("RecentlyViewed");
    recentlyViewedModule.setRenderType(RenderType.ENTITY_LIST);
    recentlyViewedModule.setContent(recentlyViewed.stream().map(this::createEntityRec).collect(Collectors.toList()));

    // Popular entities
    List<String> popularEntities = ImmutableList.of("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,logging_events,PROD)", "urn:li:dataFlow:(airflow,dag_abc,PROD)",
        "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,user_analytics)");
    RecommendationModule popularEntitiesModule = new RecommendationModule();
    popularEntitiesModule.setTitle("Most Popular");
    popularEntitiesModule.setModuleType("PopularEntities");
    popularEntitiesModule.setRenderType(RenderType.ENTITY_LIST);
    popularEntitiesModule.setContent(popularEntities.stream().map(this::createEntityRec).collect(Collectors.toList()));

    return RecommendationResults.builder()
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
    content.setLandingPageContext(SearchPageContext.builder()
        .setQuery("")
        .setFilters(ImmutableList.of(Filter.builder().setField("platform").setValue(platformUrn).build()))
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
    content.setLandingPageContext(EntityPageContext.builder().setEntityUrn(entityUrn).build());
    return content;
  }
}
