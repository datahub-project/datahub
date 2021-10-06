package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;

import java.util.Objects;
import javax.annotation.Nonnull;


public class ChangeResult {
  public final ChangeState changeState;
  public final RecordTemplate aspect;
  public final String message;

  private ChangeResult(RecordTemplate aspect, ChangeState changeState, String message) {
    this.aspect = aspect;
    this.message = message;
    this.changeState = changeState;
  }

  /**
   * The change should be allowed to continue passing on the modified aspect
   */
  public static ChangeResult success(@Nonnull RecordTemplate aspect) {
    return new ChangeResult(aspect, ChangeState.SUCCESS, null);
  }

  /**
   * The change should not be allowed to proceed
   */
  public static ChangeResult failure(String message) {
    return new ChangeResult(null, ChangeState.FAILURE, message);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChangeResult that = (ChangeResult) o;
    return changeState == that.changeState && Objects.equals(aspect, that.aspect) && Objects.equals(message,
        that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeState, aspect, message);
  }
}
