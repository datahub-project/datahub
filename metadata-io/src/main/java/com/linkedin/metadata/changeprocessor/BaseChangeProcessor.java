package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;

import java.util.ArrayList;

public class BaseChangeProcessor implements ChangeProcessor {

  @Override
  public ProcessChangeResult beforeChange(String aspectName,
      RecordTemplate previousAspect,
      RecordTemplate newAspect) {
    return new ProcessChangeResult(newAspect, ChangeState.CONTINUE, new ArrayList<>());
  }

  @Override
  public ProcessChangeResult afterChange(String aspectName,
      RecordTemplate previousAspect,
      RecordTemplate newAspect) {
    return new ProcessChangeResult(newAspect,ChangeState.CONTINUE, new ArrayList<>());
  }
}
