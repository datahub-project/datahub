package com.linkedin.metadata.processor;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.Aspect;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Responsible for routing events to the correct registered change processors. Processors are registered based on the
 * entityName plus the aspectName they apply to. If none are supplied then the original change is returned.
 */
public class ChangeStreamProcessor {

  private final Map<String, SortedSet<ChangeProcessor>> _changeProcessors;
  private final TreeSet<ChangeProcessor> EMPTY_SET = new TreeSet<>(Comparator.comparing(p -> p.priority));

  public ChangeStreamProcessor(Map<String, SortedSet<ChangeProcessor>> changeProcessors) {
    _changeProcessors = changeProcessors;
  }

  public ProcessChangeResult preProcess(String entityName,
                                        String aspectName,
                                        Aspect aspectKey,
                                        Aspect previousAspect,
                                        Aspect newAspect,
                                        ChangeType changeType) {
    return process(entityName,aspectName, aspectKey, previousAspect, newAspect, changeType, ProcessType.PRE);
  }

  public ProcessChangeResult postProcess(String entityName,
                          String aspectName,
                          Aspect aspectKey,
                          Aspect previousAspect,
                          Aspect newAspect,
                          ChangeType changeType) {
    return process(entityName,aspectName, aspectKey, previousAspect, newAspect, changeType, ProcessType.POST);
  }

  private SortedSet<ChangeProcessor> getChangeProcessors(String entityName, String aspectName) {
    String entityAspectProcessorKey = entityName + aspectName;

    return _changeProcessors.getOrDefault(entityAspectProcessorKey, EMPTY_SET);
  }

  private ProcessChangeResult process(String entityName,
                                      String aspectName,
                                      Aspect aspectKey,
                                      Aspect previousAspect,
                                      Aspect newAspect,
                                      ChangeType changeType,
                                      ProcessType processType
  ) {
    SortedSet<ChangeProcessor> changeProcessors = getChangeProcessors(entityName, aspectName);

    if (changeProcessors.isEmpty()) {
      return new ProcessChangeResult(newAspect, true, new ArrayList<>());
    }

    Aspect modifiedAspect = newAspect;
    List<String> messages = new ArrayList<>();
    boolean isChangeValid = true;

    // 2. For each registered process starting from the highest priority process the change
    for (ChangeProcessor processor : changeProcessors) {
      ProcessChangeResult result;

      if (processType == ProcessType.PRE) {
        result = processor.beforeChange(entityName, aspectKey, aspectName, previousAspect, modifiedAspect, changeType);
      } else {
        result = processor.afterChange(entityName, aspectKey, aspectName, previousAspect, modifiedAspect, changeType);
      }

      if (result.isChangeValid) {
        isChangeValid = false;
        break;
      }
      modifiedAspect = result.getAspect();
    }

    return new ProcessChangeResult(modifiedAspect, isChangeValid, messages);
  }

  private enum ProcessType {
    PRE,
    POST
  }
}
