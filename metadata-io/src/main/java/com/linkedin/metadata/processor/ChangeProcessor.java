package com.linkedin.metadata.processor;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.Aspect;

public interface ChangeProcessor {
  ProcessChangeResult beforeChange(String entityType,
                                   Aspect key,
                                   String aspectName,
                                   Aspect previousAspect,
                                   Aspect newAspect,
                                   ChangeType changeType);

  ProcessChangeResult afterChange(String entityType,
                                  Aspect key,
                                  String aspectName,
                                  Aspect previousAspect,
                                  Aspect newAspect,
                                  ChangeType changeType);

  Integer priority = 0;
}


