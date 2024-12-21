package com.linkedin.metadata.service;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;

import com.google.common.collect.ImmutableSet;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BusinessAttributeUpdateHookService {
  private static final String BUSINESS_ATTRIBUTE_OF = "BusinessAttributeOf";

  private final UpdateIndicesService updateIndicesService;
  private final int relatedEntitiesCount;
  private final int getRelatedEntitiesBatchSize;
  private ExecutorService executor;
  public static final String TAG = "TAG";
  public static final String GLOSSARY_TERM = "GLOSSARY_TERM";
  public static final String DOCUMENTATION = "DOCUMENTATION";
  private final int threadCount;
  private final int AWAIT_TERMINATION_TIME = 10;
  private final int keepAlive;

  public BusinessAttributeUpdateHookService(
      @NonNull UpdateIndicesService updateIndicesService,
      @NonNull @Value("${businessAttribute.fetchRelatedEntitiesCount}") int relatedEntitiesCount,
      @NonNull @Value("${businessAttribute.fetchRelatedEntitiesBatchSize}") int relatedBatchSize,
      @NonNull @Value("${businessAttribute.threadCount}") int threadCount,
      @NonNull @Value("${businessAttribute.keepAliveTime}") int keepAlive) {
    this.updateIndicesService = updateIndicesService;
    this.relatedEntitiesCount = relatedEntitiesCount;
    this.getRelatedEntitiesBatchSize = relatedBatchSize;
    this.threadCount = threadCount;
    this.keepAlive = keepAlive;
  }

  public void handleChangeEvent(
      @NonNull final OperationContext opContext, @NonNull final PlatformEvent event) {
    final EntityChangeEvent entityChangeEvent =
        GenericRecordUtils.deserializePayload(
            event.getPayload().getValue(), EntityChangeEvent.class);

    executor = businessAttributePropagationWorkerPool(threadCount, keepAlive);

    if (!entityChangeEvent.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
      return;
    }

    final Set<String> businessAttributeCategories =
        ImmutableSet.of(TAG, GLOSSARY_TERM, DOCUMENTATION);
    if (!businessAttributeCategories.contains(entityChangeEvent.getCategory())) {
      return;
    }

    Urn urn = entityChangeEvent.getEntityUrn();
    log.info("Business Attribute update hook invoked for urn : {}", urn);
    fetchRelatedEntities(
        opContext,
        urn,
        (batch, batchNumber, entityKey) -> processBatch(opContext, batch, batchNumber, entityKey),
        null,
        0,
        1);

    executor.shutdown();
    try {
      if (!executor.awaitTermination(AWAIT_TERMINATION_TIME, TimeUnit.MINUTES)) {
        executor.shutdownNow(); // Cancel currently executing tasks
        if (!executor.awaitTermination(AWAIT_TERMINATION_TIME, TimeUnit.MINUTES))
          log.error("Business Attribute Propagation Executor is not terminating");
      }
    } catch (InterruptedException ie) {
      executor.shutdownNow();
    }
  }

  private void fetchRelatedEntities(
      @NonNull final OperationContext opContext,
      @NonNull final Urn urn,
      @NonNull
          final TriFunction<RelatedEntitiesScrollResult, Integer, String, Callable<ExecutionResult>>
              resultFunction,
      @Nullable String scrollId,
      int consumedEntityCount,
      int batchNumber) {
    GraphRetriever graph = opContext.getRetrieverContext().getGraphRetriever();
    final ArrayList<Future<ExecutionResult>> futureList = new ArrayList<>();
    RelatedEntitiesScrollResult result =
        graph.scrollRelatedEntities(
            null,
            newFilter("urn", urn.toString()),
            null,
            EMPTY_FILTER,
            Arrays.asList(BUSINESS_ATTRIBUTE_OF),
            newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
            Edge.EDGE_SORT_CRITERION,
            scrollId,
            getRelatedEntitiesBatchSize,
            null,
            null);

    futureList.add(
        executor.submit(resultFunction.apply(result, batchNumber, urn.getEntityKey().toString())));

    consumedEntityCount = consumedEntityCount + result.getEntities().size();
    if (result.getScrollId() != null && consumedEntityCount < relatedEntitiesCount) {
      batchNumber = batchNumber + 1;
      fetchRelatedEntities(
          opContext, urn, resultFunction, result.getScrollId(), consumedEntityCount, batchNumber);
    }

    for (Future<ExecutionResult> future : futureList) {
      try {
        ExecutionResult futureResult = future.get();
        if (futureResult.getException() != null) {
          log.error(
              "Batch {} for BA:{} is failed with exception",
              futureResult.getBatchNumber(),
              futureResult.getEntityKey(),
              futureResult.getException());
        } else {
          log.info(futureResult.getResult());
        }
      } catch (InterruptedException | ExecutionException e) {
        log.error("Business Attribute Propagation Parallel Processing Exception", e);
      }
    }
    futureList.clear();
  }

  private Callable<ExecutionResult> processBatch(
      @NonNull OperationContext opContext,
      @NonNull RelatedEntitiesScrollResult batch,
      int batchNumber,
      String entityKey) {
    return () -> {
      StopWatch stopWatch = new StopWatch();
      stopWatch.start();
      AspectRetriever aspectRetriever = opContext.getAspectRetriever();
      log.info("Batch {} for BA:{} started", batchNumber, entityKey);
      ExecutionResult executionResult = new ExecutionResult();
      executionResult.setBatchNumber(batchNumber);
      executionResult.setEntityKey(entityKey);
      try {
        Set<Urn> entityUrns =
            batch.getEntities().stream()
                .map(RelatedEntity::getUrn)
                .map(UrnUtils::getUrn)
                .collect(Collectors.toSet());

        Map<Urn, Map<String, Aspect>> entityAspectMap =
            aspectRetriever.getLatestAspectObjects(
                entityUrns, Set.of(Constants.BUSINESS_ATTRIBUTE_ASPECT));

        entityAspectMap.entrySet().stream()
            .filter(entry -> entry.getValue().containsKey(Constants.BUSINESS_ATTRIBUTE_ASPECT))
            .forEach(
                entry -> {
                  final Urn entityUrn = entry.getKey();
                  final Aspect aspect = entry.getValue().get(Constants.BUSINESS_ATTRIBUTE_ASPECT);
                  updateIndicesService.handleChangeEvent(
                      opContext,
                      PegasusUtils.constructMCL(
                          null,
                          Constants.SCHEMA_FIELD_ENTITY_NAME,
                          entityUrn,
                          ChangeType.UPSERT,
                          Constants.BUSINESS_ATTRIBUTE_ASPECT,
                          opContext.getAuditStamp(),
                          new BusinessAttributes(aspect.data()),
                          null,
                          null,
                          null));
                });
        stopWatch.stop();
        String result =
            String.format(
                    "Batch %s for BA:%s is completed in %s",
                    batchNumber, entityKey, TimeAgo.toDuration(stopWatch.getTime()))
                .toString();
        executionResult.setResult(result);
      } catch (Exception e) {
        executionResult.setException(e);
      }
      return executionResult;
    };
  }

  private ExecutorService businessAttributePropagationWorkerPool(int numThreads, int keepAlive) {
    numThreads = numThreads < 0 ? Runtime.getRuntime().availableProcessors() * 2 : numThreads;
    return new ThreadPoolExecutor(
        numThreads, numThreads, keepAlive, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
  }

  @FunctionalInterface
  private interface TriFunction<T, U, V, R> {
    R apply(T t, U u, V v);
  }

  @Data
  private class ExecutionResult {
    String result;
    Throwable exception;
    int batchNumber;
    String entityKey;
  }

  private static final class TimeAgo {
    private static final List<Long> times =
        Arrays.asList(
            TimeUnit.DAYS.toMillis(365),
            TimeUnit.DAYS.toMillis(30),
            TimeUnit.DAYS.toMillis(1),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.MINUTES.toMillis(1),
            TimeUnit.SECONDS.toMillis(1),
            TimeUnit.MILLISECONDS.toMillis(1));
    private static final List<String> timesString =
        Arrays.asList("year", "month", "day", "hour", "minute", "second", "milliseconds");

    private static String toDuration(long duration) {

      StringBuffer res = new StringBuffer();
      for (int i = 0; i < times.size(); i++) {
        Long current = times.get(i);
        long temp = duration / current;
        if (temp > 0) {
          res.append(temp)
              .append(" ")
              .append(timesString.get(i))
              .append(temp != 1 ? "s" : StringUtils.EMPTY)
              .append(" ");
        }
        duration = duration % current;
      }
      if (StringUtils.EMPTY.equals(res.toString())) return "0 seconds ago";
      else return res.toString();
    }
  }
}
