package com.linkedin.metadata.changeprocessor;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Responsible for routing events to the correct registered change processors. Processors are registered based on the
 * entityName plus the aspectName they apply to. If none are supplied then the original change is returned.
 */
public class ChangeStreamProcessor {

  private final Comparator<ChangeProcessor> PROCESSOR_COMPARATOR = Comparator.comparing(p -> p.priority);
  private final TreeSet<ChangeProcessor> EMPTY_SET = new TreeSet<>(PROCESSOR_COMPARATOR);
  private final Map<String, SortedSet<ChangeProcessor>> _changeProcessors = new HashMap<>();

  public ChangeStreamProcessor() {
  }

  public void registerProcessor(String aspectName, ChangeProcessor processor) {
    if (_changeProcessors.containsKey(aspectName)) {
      _changeProcessors.get(aspectName).add(processor);
    } else {
      _changeProcessors.put(aspectName, new TreeSet<>(PROCESSOR_COMPARATOR));
    }
  }

  public ProcessChangeResult preProcess(String aspectName,
      RecordTemplate latestAspect,
      RecordTemplate newAspect,
      AuditStamp auditStamp,
      SystemMetadata systemMetadata) {
    return process(aspectName, latestAspect, newAspect, auditStamp, systemMetadata, ProcessType.PRE);
  }

  public ProcessChangeResult postProcess(String aspectName,
      RecordTemplate latestAspect,
      RecordTemplate newAspect,
      AuditStamp auditStamp,
      SystemMetadata systemMetadata) {
    return process(aspectName, latestAspect, newAspect, auditStamp, systemMetadata, ProcessType.POST);
  }

  private ProcessChangeResult process(String aspectName,
      RecordTemplate latestAspect,
      RecordTemplate newAspect,
      AuditStamp auditStamp,
      SystemMetadata systemMetadata,
      ProcessType processType
  ) {
    SortedSet<ChangeProcessor> changeProcessors = _changeProcessors.getOrDefault(aspectName, EMPTY_SET);

    if (changeProcessors.isEmpty()) {
      return new ProcessChangeResult(newAspect, ChangeState.CONTINUE, new ArrayList<>());
    }

    List<String> messages = new ArrayList<>();
    ProcessChangeResult result = new ProcessChangeResult(newAspect, ChangeState.CONTINUE, messages);

    for (ChangeProcessor processor : changeProcessors) {
      if (processType == ProcessType.PRE) {
        result = processor.beforeChange(aspectName, latestAspect, result.getAspect(), auditStamp, systemMetadata);
      } else {
        result = processor.afterChange(aspectName, latestAspect, result.getAspect(), auditStamp, systemMetadata);
      }
      messages.addAll(result.getMessages());
      //TODO HANDLE CHANGE STATE
    }

    return new ProcessChangeResult(result.getAspect(), result.changeState, messages);
  }

  private enum ProcessType {
    PRE,
    POST
  }
}
