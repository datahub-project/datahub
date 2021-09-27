package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * Responsible for routing events to the correct registered change processors. Processors are registered based on the
 * aspectName they apply to. If none are supplied then the proposed change is returned.
 */
@Slf4j
public class ChangeStreamProcessor {

  private final Comparator<ChangeProcessor> _processorComparator = Comparator.comparing(p -> p.PRIORITY);
  private final TreeSet<ChangeProcessor> _emptySet = new TreeSet<>(_processorComparator);
  private final Map<String, SortedSet<ChangeProcessor>> _beforeChangeProcessors = new HashMap<>();
  private final Map<String, SortedSet<ChangeProcessor>> _afterChangeProcessors = new HashMap<>();

  public void addBeforeChangeProcessor(String entityName, String aspectName, ChangeProcessor processor) {
    addProcessor(entityName, aspectName, processor, _beforeChangeProcessors);
  }

  public void addAfterChangeProcessor(String entityName, String aspectName, ChangeProcessor processor) {
    addProcessor(entityName, aspectName, processor, _afterChangeProcessors);
  }

  public ProcessChangeResult runBeforeChangeProcessors(String entityName, String aspectName,
      RecordTemplate previousAspect, RecordTemplate newAspect) {
    return process(entityName, aspectName, previousAspect, newAspect, _beforeChangeProcessors);
  }

  public ProcessChangeResult runAfterChangeProcessors(String entityName, String aspectName,
      RecordTemplate previousAspect, RecordTemplate newAspect) {
    return process(entityName, aspectName, previousAspect, newAspect, _afterChangeProcessors);
  }

  private void addProcessor(String entityName, String aspectName, ChangeProcessor processor,
      Map<String, SortedSet<ChangeProcessor>> processorMap) {
    String processorKey = (entityName + aspectName).toLowerCase();

    if (processorMap.containsKey(processorKey)) {
      processorMap.get(processorKey).add(processor);
    } else {
      TreeSet<ChangeProcessor> processors = new TreeSet<>(_processorComparator);
      processors.add(processor);
      processorMap.put(processorKey, processors);
    }
  }

  private ProcessChangeResult process(String entityName, String aspectName, RecordTemplate previousAspect,
      RecordTemplate newAspect, Map<String, SortedSet<ChangeProcessor>> processorMap) {

    String processorKey = (entityName + aspectName).toLowerCase();
    SortedSet<ChangeProcessor> changeProcessors = processorMap.getOrDefault(processorKey, _emptySet);

    if (changeProcessors.isEmpty()) {
      return new ProcessChangeResult(newAspect, ChangeState.CONTINUE, new ArrayList<>());
    }

    List<String> messages = new ArrayList<>();
    ProcessChangeResult result = new ProcessChangeResult(newAspect, ChangeState.CONTINUE, messages);

    for (ChangeProcessor processor : changeProcessors) {
      result = processor.process(entityName, aspectName, previousAspect, result.getAspect());
      messages.addAll(result.getMessages());
    }

    return new ProcessChangeResult(result.getAspect(), result.changeState, messages);
  }
}
