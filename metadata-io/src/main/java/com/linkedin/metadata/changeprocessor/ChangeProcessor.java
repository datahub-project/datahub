package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


public interface ChangeProcessor {
  /**
   * Given a proposed change to an aspect on an entity run some
   * @param entityName
   * @param aspectName
   * @param previousAspect
   * @param newAspect
   * @return
   */
  @Nonnull
  ProcessChangeResult process(String entityName, String aspectName, RecordTemplate previousAspect,
      RecordTemplate newAspect);

  default Integer getPriority(){
    return 0;
  }
}


