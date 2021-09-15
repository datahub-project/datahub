package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.Aspect;

public class BaseChangeProcessor implements ChangeProcessor {

  @Override
  public ProcessChangeResult beforeChange(String aspectName,
      RecordTemplate latestAspect,
      RecordTemplate modifiedAspect,
      RecordTemplate previousAspect,
      RecordTemplate newAspect) {
    return null;
  }

  @Override
  public ProcessChangeResult afterChange(String aspectName,
      RecordTemplate latestAspect,
      RecordTemplate modifiedAspect,
      RecordTemplate previousAspect,
      RecordTemplate newAspect) {
    return null;
  }
}
