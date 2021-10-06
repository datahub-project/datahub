package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


public interface ChangeProcessor {
  @Nonnull
  ChangeResult process(String entityName, String aspectName, RecordTemplate previousAspect, RecordTemplate newAspect);

  /**
   * The order in which processors are run on the change. 0 is the highest priority and will be run first.
   * @return priority of the processor
   */
  default Integer getPriority() {
    return 0;
  }
}
