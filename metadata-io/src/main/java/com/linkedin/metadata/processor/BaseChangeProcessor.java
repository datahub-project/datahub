package com.linkedin.metadata.processor;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.Aspect;

public class BaseChangeProcessor implements ChangeProcessor {
  @Override
  public ProcessChangeResult beforeChange(String entityType,
                                          Aspect key,
                                          String aspectName,
                                          Aspect previousAspect,
                                          Aspect newAspect,
                                          ChangeType changeType) {
    return null;
  }

  @Override
  public ProcessChangeResult afterChange(String entityType,
                                         Aspect key,
                                         String aspectName,
                                         Aspect previousAspect,
                                         Aspect newAspect,
                                         ChangeType changeType) {
    return null;
  }
}
