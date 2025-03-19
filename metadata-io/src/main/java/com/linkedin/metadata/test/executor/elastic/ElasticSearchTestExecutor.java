package com.linkedin.metadata.test.executor.elastic;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.test.util.TestUtils.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.test.BatchTestRunResult;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
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
  private final OperationContext opContext;
  private final int executionLimit;

  public ElasticSearchTestExecutor(
      EntitySearchService searchService,
      TimeseriesAspectService timeseriesAspectService,
      EntityRegistry entityRegistry,
      OperationContext opContext,
      int executionLimit) {
    this.searchService = searchService;
    this.timeseriesAspectService = timeseriesAspectService;
    this.convertor = new ElasticTestDefinitionConvertor(entityRegistry);
    this.opContext = opContext;
    this.executionLimit = executionLimit;
  }

  public boolean canSelect(TestDefinition testDefinition) {
    return convertor.canSelect(testDefinition);
  }

  public boolean canEvaluate(TestDefinition testDefinition) {
    return convertor.canEvaluate(testDefinition);
  }

  public List<String> explainSelect(TestDefinition testDefinition) {
    return convertor.explainSelect(testDefinition);
  }

  public List<String> explainEvaluate(TestDefinition testDefinition) {
    return convertor.explainEvaluate(testDefinition);
  }

  public List<Urn> select(TestDefinition testDefinition, @Nonnull final String query) {
    ElasticTestDefinition elasticTestDefinition = convertor.convert(testDefinition);
    SearchResult searchResult =
        searchService.predicateSearch(
            opContext,
            elasticTestDefinition.getSelectedEntityTypes(),
            query,
            elasticTestDefinition.getSelectionFilters(),
            null,
            0,
            executionLimit,
            Collections.emptyList());
    if (searchResult.getNumEntities() >= executionLimit) {
      throw abortBeyondLimitExecution(testDefinition, searchResult.getNumEntities());
    }
    return searchResult.getEntities().stream()
        .map(SearchEntity::getEntity)
        .collect(Collectors.toList());
  }

  public Map<Urn, TestResults> evaluate(
      TestDefinition testDefinition,
      @Nonnull String query,
      @Nonnull final BatchTestRunResult batchTestRunResult,
      final boolean shouldExecuteBatchBeyondLimit) {
    ElasticTestDefinition elasticTestDefinition = convertor.convert(testDefinition);
    TestResultArray emptyResults = new TestResultArray();
    Map<Urn, TestResults> results = new HashMap<>();
    AtomicInteger passing = new AtomicInteger();
    AtomicInteger failing = new AtomicInteger();
    AuditStamp currentTime = null;
    currentTime =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR));
    List<String> entityTypes = elasticTestDefinition.getSelectedEntityTypes();
    Set<Urn> passingUrns = new HashSet<>();
    TestResultArray passingResults = new TestResultArray();
    passingResults.add(
        new TestResult()
            .setTest(testDefinition.getUrn())
            .setTestDefinitionMd5(testDefinition.getMd5())
            .setType(TestResultType.SUCCESS)
            .setLastComputed(currentTime));
    Predicate passingFilters = elasticTestDefinition.getPassingFilters();
    log.info(
        "Evaluating test {} for entity types {} with filters {}",
        testDefinition.getUrn(),
        entityTypes,
        passingFilters);
    SearchResult passingSearchResult =
        searchService.predicateSearch(
            opContext,
            entityTypes,
            query,
            passingFilters,
            null,
            0,
            executionLimit,
            Collections.emptyList());
    if (!shouldExecuteBatchBeyondLimit && passingSearchResult.getNumEntities() >= executionLimit) {
      throw abortBeyondLimitExecution(testDefinition, passingSearchResult.getNumEntities());
    }
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
    if (elasticTestDefinition.getFailingFilters() != null) {
      failingEntities =
          searchService
              .predicateSearch(
                  opContext,
                  entityTypes,
                  query,
                  elasticTestDefinition.getFailingFilters(),
                  null,
                  0,
                  executionLimit,
                  Collections.emptyList())
              .getEntities()
              .stream();
    } else {
      SearchResult selectionSearchResult =
          searchService.predicateSearch(
              opContext,
              entityTypes,
              query,
              elasticTestDefinition.getSelectionFilters(),
              null,
              0,
              executionLimit,
              Collections.emptyList());
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
    batchTestRunResult.setPassingCount(passing.get()).setFailingCount(failing.get());
    return results;
  }
}
