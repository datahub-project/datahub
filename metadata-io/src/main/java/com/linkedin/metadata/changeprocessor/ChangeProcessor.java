package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.Aspect;

public interface ChangeProcessor {
  ProcessChangeResult beforeChange(String aspectName,
      RecordTemplate latestAspect, RecordTemplate modifiedAspect, RecordTemplate previousAspect,
      RecordTemplate newAspect
  );

  ProcessChangeResult afterChange(
      String aspectName,
      RecordTemplate latestAspect, RecordTemplate modifiedAspect, RecordTemplate previousAspect,
      RecordTemplate newAspect
  );

  Integer priority = 0;
}


