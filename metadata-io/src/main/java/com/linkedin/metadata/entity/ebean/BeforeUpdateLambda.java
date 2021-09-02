package com.linkedin.metadata.entity.ebean;

import com.linkedin.data.template.RecordTemplate;

import java.util.Optional;
import java.util.function.BiFunction;

public class BeforeUpdateLambda implements BiFunction<Optional<RecordTemplate>, RecordTemplate, Optional<RecordTemplate>> {
  private BiFunction<Optional<RecordTemplate>, RecordTemplate, Optional<RecordTemplate>> lambda;

  public BeforeUpdateLambda(BiFunction<Optional<RecordTemplate>, RecordTemplate, Optional<RecordTemplate>> lambda){
    this.lambda = lambda;
  }

  @Override
  public Optional<RecordTemplate> apply(Optional<RecordTemplate> oldRecordTemplate, RecordTemplate newRecordTemplate) {
    return lambda.apply(oldRecordTemplate, newRecordTemplate);
  }
}
