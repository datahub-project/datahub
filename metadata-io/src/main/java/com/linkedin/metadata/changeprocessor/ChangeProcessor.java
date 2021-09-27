package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;


public interface ChangeProcessor {
  /**
   * Given a proposed change to an aspect on an entity run some
   * @param entityName
   * @param aspectName
   * @param previousAspect
   * @param newAspect
   * @return
   */
  ProcessChangeResult process(String entityName, String aspectName, RecordTemplate previousAspect,
      RecordTemplate newAspect);

  Integer PRIORITY = 0;
}


