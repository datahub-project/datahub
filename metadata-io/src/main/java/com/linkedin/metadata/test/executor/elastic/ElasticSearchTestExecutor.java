package com.linkedin.metadata.test.executor.elastic;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.test.BatchTestRunResult;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticSearchTestExecutor {

  private final EntitySearchService searchService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final ElasticTestDefinitionConvertor convertor;

  public ElasticSearchTestExecutor(
      EntitySearchService searchService,
      TimeseriesAspectService timeseriesAspectService,
      EntityRegistry entityRegistry) {
    this.searchService = searchService;
    this.timeseriesAspectService = timeseriesAspectService;
    this.convertor = new ElasticTestDefinitionConvertor(entityRegistry);
  }

  public boolean canSelect(TestDefinition testDefinition) {
    return convertor.canSelect(testDefinition);
  }

  public boolean canEvaluate(TestDefinition testDefinition) {
    return convertor.canEvaluate(testDefinition);
  }

  public List<Urn> select(TestDefinition testDefinition) {
    ElasticTestDefinition elasticTestDefinition = convertor.convert(testDefinition);
    return elasticTestDefinition.getSelectedEntityTypes().stream()
        .flatMap(
            entityType ->
                searchService
                    .search(
                        List.of(entityType),
                        "*",
                        elasticTestDefinition.getSelectionFilters(entityType),
                        null,
                        0,
                        1000,
                        null)
                    .getEntities()
                    .stream()
                    .map(SearchEntity::getEntity))
        .collect(Collectors.toList());
  }

  public Map<Urn, TestResults> evaluate(
      TestDefinition testDefinition, @Nonnull final BatchTestRunResult batchTestRunResult) {
    ElasticTestDefinition elasticTestDefinition = convertor.convert(testDefinition);
    TestResultArray emptyResults = new TestResultArray();
    Map<Urn, TestResults> results = new HashMap<>();
    AtomicInteger passing = new AtomicInteger();
    AtomicInteger failing = new AtomicInteger();
    AuditStamp currentTime = null;
    try {
      currentTime =
          new AuditStamp()
              .setTime(System.currentTimeMillis())
              .setActor(Urn.createFromString("urn:li:actor:system"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    for (String entityType : elasticTestDefinition.getSelectedEntityTypes()) {
      Set<Urn> passingUrns = new HashSet<Urn>();
      TestResultArray passingResults = new TestResultArray();
      passingResults.add(
          new TestResult()
              .setTest(testDefinition.getUrn())
              .setTestDefinitionMd5(testDefinition.getMd5())
              .setType(TestResultType.SUCCESS)
              .setLastComputed(currentTime));
      Filter passingFilters = elasticTestDefinition.getPassingFilters(entityType);
      log.info(
          "Evaluating test {} for entity type {} with filters {}",
          testDefinition.getUrn(),
          entityType,
          passingFilters);
      SearchResult passingSearchResult =
          searchService.search(List.of(entityType), "*", passingFilters, null, 0, 1000, null);
      passingSearchResult
          .getEntities()
          .forEach(
              entity -> {
                passing.incrementAndGet();
                passingUrns.add(entity.getEntity());
                results.put(
                    entity.getEntity(),
                    new TestResults().setPassing(passingResults).setFailing(emptyResults));
              });
      TestResultArray failingResults = new TestResultArray();
      failingResults.add(
          new TestResult()
              .setTest(testDefinition.getUrn())
              .setTestDefinitionMd5(testDefinition.getMd5())
              .setType(TestResultType.FAILURE)
              .setLastComputed(currentTime));
      Stream<SearchEntity> failingEntities;
      if (elasticTestDefinition.getFailingFilters(entityType) != null) {
        failingEntities =
            searchService
                .search(
                    List.of(entityType),
                    "*",
                    elasticTestDefinition.getFailingFilters(entityType),
                    null,
                    0,
                    1000,
                    null)
                .getEntities()
                .stream();
      } else {
        Filter selectionFilter = elasticTestDefinition.getSelectionFilters(entityType);
        SearchResult selectionSearchResult =
            searchService.search(
                List.of(entityType),
                "*",
                elasticTestDefinition.getSelectionFilters(entityType),
                null,
                0,
                1000,
                null);
        failingEntities =
            selectionSearchResult.getEntities().stream()
                .filter(entity -> !passingUrns.contains(entity.getEntity()));
      }
      failingEntities.forEach(
          entity -> {
            if (!passingUrns.contains(entity.getEntity())) {
              failing.incrementAndGet();
              results.put(
                  entity.getEntity(),
                  new TestResults().setFailing(failingResults).setPassing(emptyResults));
            }
          });
    }
    batchTestRunResult.setPassingCount(passing.get()).setFailingCount(failing.get());
    return results;
  }
}
