package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;

import java.util.List;


public class ProcessChangeResult {
  public final ChangeState changeState;
  private final RecordTemplate aspect;
  private final List<String> messages;

  public ProcessChangeResult(RecordTemplate aspect, ChangeState changeState, List<String> messages) {
    this.aspect = aspect;
    this.messages = messages;
    this.changeState = changeState;
  }

  public List<String> getMessages() {
    return messages;
  }

  public RecordTemplate getAspect() {
    return aspect;
  }

  public ChangeState getChangeState() {
    return changeState;
  }
}
