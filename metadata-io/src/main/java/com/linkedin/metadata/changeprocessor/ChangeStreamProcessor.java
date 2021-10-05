package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * Responsible for routing events to the correct registered change processors. Processors are registered based on the
 * aspectName they apply to. If none are supplied then the proposed change is returned.
 */
@Slf4j
public class ChangeStreamProcessor {

  private final Comparator<ChangeProcessor> _processorComparator = Comparator.comparing(ChangeProcessor::getPriority);
  private final Map<String, SortedSet<ChangeProcessor>> _beforeChangeProcessors = new HashMap<>();
  private final Map<String, SortedSet<ChangeProcessor>> _afterChangeProcessors = new HashMap<>();

  public void addBeforeChangeProcessor(String entityName, String aspectName, ChangeProcessor processor) {
    addProcessor(entityName, aspectName, processor, _beforeChangeProcessors);
  }

  public void addAfterChangeProcessor(String entityName, String aspectName, ChangeProcessor processor) {
    addProcessor(entityName, aspectName, processor, _afterChangeProcessors);
  }

  public ProcessChangeResult processBeforeChange(String entityName, String aspectName, RecordTemplate previousAspect,
      RecordTemplate newAspect) {
    return process(entityName, aspectName, previousAspect, newAspect, _beforeChangeProcessors);
  }

  public ProcessChangeResult processAfterChange(String entityName, String aspectName, RecordTemplate previousAspect,
      RecordTemplate newAspect) {
    return process(entityName, aspectName, previousAspect, newAspect, _afterChangeProcessors);
  }

  private void addProcessor(String entityName, String aspectName, ChangeProcessor processor,
      Map<String, SortedSet<ChangeProcessor>> processorMap) {
    String processorKey = (entityName + "/" + aspectName).toLowerCase();

    if (processorMap.containsKey(processorKey)) {
      processorMap.get(processorKey).add(processor);
    } else {
      TreeSet<ChangeProcessor> processors = new TreeSet<>(_processorComparator);
      processors.add(processor);
      processorMap.put(processorKey, processors);
    }
  }

  @Nonnull
  private ProcessChangeResult process(String entityName, String aspectName, RecordTemplate previousAspect,
      RecordTemplate newAspect, Map<String, SortedSet<ChangeProcessor>> processorMap) {

    SortedSet<ChangeProcessor> processors =
        getChangeProcessors(entityName.toLowerCase(), aspectName.toLowerCase(), processorMap);

    if (processors.isEmpty()) {
      return ProcessChangeResult.success(newAspect);
    }

    RecordTemplate modifiedAspect = newAspect;
    ProcessChangeResult processedResult = ProcessChangeResult.success(newAspect);

    for (ChangeProcessor processor : processors) {
      processedResult = processor.process(entityName, aspectName, previousAspect, modifiedAspect);
      modifiedAspect = processedResult.aspect;

      switch (processedResult.changeState) {
        case BLOCKER:
        case FAILURE:
          break;
        default:
      }
    }

    return processedResult;
  }

  private SortedSet<ChangeProcessor> getChangeProcessors(String entityName, String aspectName,
      Map<String, SortedSet<ChangeProcessor>> processorMap) {

    // 1. Get all processors that apply to all entities/aspects
    SortedSet<ChangeProcessor> allEntityAspectProcessors =
        processorMap.getOrDefault("*/*", new TreeSet<>(_processorComparator));

    // 2. Get entity specific processors
    SortedSet<ChangeProcessor> entityProcessors =
        processorMap.getOrDefault(entityName + "/*", new TreeSet<>(_processorComparator));

    // 3. Get aspect specific processors
    SortedSet<ChangeProcessor> aspectProcessors =
        processorMap.getOrDefault(entityName + "/" + aspectName, new TreeSet<>(_processorComparator));

    // 4. Combine all processors sorting by priority
    TreeSet<ChangeProcessor> processors = new TreeSet<>(_processorComparator);
    processors.addAll(allEntityAspectProcessors);
    processors.addAll(entityProcessors);
    processors.addAll(aspectProcessors);

    return processors;
  }
}
