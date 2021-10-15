package com.linkedin.datahub.graphql.resolvers.recommendation;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityProfileParams;
import com.linkedin.datahub.graphql.generated.Filter;
import com.linkedin.datahub.graphql.generated.ListRecommendationsInput;
import com.linkedin.datahub.graphql.generated.ListRecommendationsResult;
import com.linkedin.datahub.graphql.generated.RecommendationContent;
import com.linkedin.datahub.graphql.generated.RecommendationModule;
import com.linkedin.datahub.graphql.generated.RecommendationParams;
import com.linkedin.datahub.graphql.generated.RecommendationRenderType;
import com.linkedin.datahub.graphql.generated.ScenarioType;
import com.linkedin.datahub.graphql.generated.SearchParams;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


@Slf4j
@RequiredArgsConstructor
public class ListRecommendationsResolver implements DataFetcher<CompletableFuture<ListRecommendationsResult>> {

  @Override
  public CompletableFuture<ListRecommendationsResult> get(DataFetchingEnvironment environment) {
    final ListRecommendationsInput input = bindArgument(environment.getArgument("input"), ListRecommendationsInput.class);

    return CompletableFuture.supplyAsync(() -> {
      try {
        log.debug("Listing recommendations for input {}", input);
        if (input.getRequestContext().getScenario() == ScenarioType.HOME) {
          return createMockHomeResults();
        } else if (input.getRequestContext().getScenario() == ScenarioType.SEARCH_RESULTS) {
          return createMockSearchResults();
        }
        return createMockEntityResults();
      } catch (Exception e) {
        log.error("Failed to get recommendations for input {}", input);
        throw new RuntimeException("Failed to get recommendations for input " + input, e);
      }
    });
  }

  private ListRecommendationsResult createMockSearchResults() {
    // Popular Tags
    List<String> popularTagList =
        ImmutableList.of(
            "urn:li:tag:Legacy",
            "urn:li:tag:Email",
            "urn:li:tag:PII",
            "urn:li:tag:Highly Confidential",
            "urn:li:tag:Marketing",
            "urn:li:tag:SalesDev",
            "urn:li:tag:MemberRefreshQ3");
    RecommendationModule popularTags = new RecommendationModule();
    popularTags.setTitle("Popular Tags");
    popularTags.setModuleType("PopularTags");
    popularTags.setRenderType(RecommendationRenderType.TAG_SEARCH_LIST);
    popularTags.setContent(popularTagList.stream().map(this::createTagSearchRec).collect(Collectors.toList()));

    return ListRecommendationsResult.builder()
        .setModules(ImmutableList.of(popularTags))
        .build();
  }

  private ListRecommendationsResult createMockEntityResults() {
    // People also viewed
    List<String> peopleAlsoViewed =
        ImmutableList.of("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)", "urn:li:dashboard:(looker,baz)",
            "urn:li:glossaryTerm:AccountBalance");
    RecommendationModule peopleAlsoViewedModule = new RecommendationModule();
    peopleAlsoViewedModule.setTitle("People also viewed");
    peopleAlsoViewedModule.setModuleType("PeopleAlsoViewed");
    peopleAlsoViewedModule.setRenderType(RecommendationRenderType.ENTITY_LIST);
    peopleAlsoViewedModule.setContent(peopleAlsoViewed.stream().map(this::createEntityRec).collect(Collectors.toList()));

    return ListRecommendationsResult.builder()
        .setModules(ImmutableList.of(peopleAlsoViewedModule))
        .build();
  }

  private ListRecommendationsResult createMockHomeResults() {
    // Top platforms
    List<String> platforms =
        ImmutableList.of("urn:li:dataPlatform:bigquery", "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:looker",
            "urn:li:dataPlatform:airflow");
    RecommendationModule topPlatformModule = new RecommendationModule();
    topPlatformModule.setTitle("Top Platforms");
    topPlatformModule.setModuleType("TopPlatforms");
    topPlatformModule.setRenderType(RecommendationRenderType.PLATFORM_LIST);
    topPlatformModule.setContent(platforms.stream().map(this::createPlatformRec).collect(Collectors.toList()));

    // Recently Viewed
    List<String> recentlyViewed = ImmutableList.of("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)", "urn:li:dashboard:(looker,baz)");
    RecommendationModule recentlyViewedModule = new RecommendationModule();
    recentlyViewedModule.setTitle("Recently Viewed");
    recentlyViewedModule.setModuleType("RecentlyViewed");
    recentlyViewedModule.setRenderType(RecommendationRenderType.ENTITY_LIST);
    recentlyViewedModule.setContent(recentlyViewed.stream().map(this::createEntityRec).collect(Collectors.toList()));

    // Popular entities
    List<String> popularEntities = ImmutableList.of("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,logging_events,PROD)", "urn:li:dataFlow:(airflow,dag_abc,PROD)",
        "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,user_analytics)");
    RecommendationModule popularEntitiesModule = new RecommendationModule();
    popularEntitiesModule.setTitle("Most Popular");
    popularEntitiesModule.setModuleType("PopularEntities");
    popularEntitiesModule.setRenderType(RecommendationRenderType.ENTITY_LIST);
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
        .setEntityProfileParams(EntityProfileParams.builder()
            .setUrn(entityUrn)
            .build())
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
