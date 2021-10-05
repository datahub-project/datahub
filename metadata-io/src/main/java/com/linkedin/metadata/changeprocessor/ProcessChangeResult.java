package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;

import java.util.Objects;
import javax.annotation.Nonnull;


public class ProcessChangeResult {
  public final ChangeState changeState;
  public final RecordTemplate aspect;
  public final String message;

  private ProcessChangeResult(RecordTemplate aspect, ChangeState changeState, String message) {
    this.aspect = aspect;
    this.message = message;
    this.changeState = changeState;
  }

  public static ProcessChangeResult success(@Nonnull RecordTemplate aspect) {
    return new ProcessChangeResult(aspect, ChangeState.SUCCESS, null);
  }

  public static ProcessChangeResult failure(String message) {
    return new ProcessChangeResult(null, ChangeState.FAILURE, message);
  }

  public static ProcessChangeResult blocker(@Nonnull RecordTemplate aspect,  String message){
    return new ProcessChangeResult(aspect, ChangeState.BLOCKER, message);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProcessChangeResult that = (ProcessChangeResult) o;
    return changeState == that.changeState && Objects.equals(aspect, that.aspect) && Objects.equals(message,
        that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeState, aspect, message);
  }
}
