package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * Responsible for routing events to the correct registered change processors. Processors are registered based on the
 * entity/aspect they apply to. If none are supplied then the proposed change is returned.
 */
@Slf4j
public class ChangeStreamProcessor {

  private final ProcessorRegistry _preProcessors = new ProcessorRegistry();
  private final ProcessorRegistry _postProcessors = new ProcessorRegistry();

  public void registerPreProcessor(String entityName, String aspectName, ChangeProcessor processor) {
    _preProcessors.registerProcessor(entityName, aspectName, processor);
  }

  public void registerPostProcessor(String entityName, String aspectName, ChangeProcessor processor) {
    _postProcessors.registerProcessor(entityName, aspectName, processor);
  }

  public ChangeResult preProcessChange(String entityName, String aspectName, RecordTemplate previousAspect,
      RecordTemplate newAspect) {
    return _preProcessors.process(entityName, aspectName, previousAspect, newAspect);
  }

  public ChangeResult postProcessChange(String entityName, String aspectName, RecordTemplate previousAspect,
      RecordTemplate newAspect) {
    return _postProcessors.process(entityName, aspectName, previousAspect, newAspect);
  }

  private class ProcessorRegistry {
    private final Comparator<ChangeProcessor> _processorComparator = Comparator.comparing(ChangeProcessor::getPriority);
    private final Map<String, SortedSet<ChangeProcessor>> _processors = new ConcurrentHashMap<>();

    private void registerProcessor(String entityName, String aspectName, ChangeProcessor processor) {
      String processorKey = (entityName + "/" + aspectName).toLowerCase();

      if (_processors.containsKey(processorKey)) {
        _processors.get(processorKey).add(processor);
      } else {
        TreeSet<ChangeProcessor> processors = new TreeSet<>(_processorComparator);
        processors.add(processor);
        _processors.put(processorKey, processors);
      }
    }

    @Nonnull
    private ChangeResult process(String entityName, String aspectName, RecordTemplate previousAspect,
                                 RecordTemplate newAspect) {

      SortedSet<ChangeProcessor> processors = getProcessors(entityName.toLowerCase(), aspectName.toLowerCase());

      if (processors.isEmpty()) {
        return ChangeResult.success(newAspect);
      }

      RecordTemplate modifiedAspect = newAspect;
      ChangeResult processedResult = ChangeResult.success(newAspect);

      for (ChangeProcessor processor : processors) {
        processedResult = processor.process(entityName, aspectName, previousAspect, modifiedAspect);
        modifiedAspect = processedResult.aspect;

        if (processedResult.changeState == ChangeState.FAILURE) {
          // If failure condition found stop processing
          break;
        }
      }

      return processedResult;
    }

    private SortedSet<ChangeProcessor> getProcessors(String entityName, String aspectName) {

      // 1. Get all processors that apply to all entities/aspects
      SortedSet<ChangeProcessor> allEntityAspectProcessors =
              _processors.getOrDefault("*/*", new TreeSet<>(_processorComparator));

      // 2. Get entity specific processors
      SortedSet<ChangeProcessor> entityProcessors =
              _processors.getOrDefault(entityName + "/*", new TreeSet<>(_processorComparator));

      // 3. Get aspect specific processors
      SortedSet<ChangeProcessor> aspectProcessors =
              _processors.getOrDefault(entityName + "/" + aspectName, new TreeSet<>(_processorComparator));

      // 4. Combine all processors sorting by priority
      TreeSet<ChangeProcessor> processors = new TreeSet<>(_processorComparator);
      processors.addAll(allEntityAspectProcessors);
      processors.addAll(entityProcessors);
      processors.addAll(aspectProcessors);

      return processors;
    }
  }
}
