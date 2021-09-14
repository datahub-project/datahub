package com.linkedin.metadata.processor;

import com.linkedin.metadata.aspect.Aspect;

import java.util.List;

public class ProcessChangeResult {
  public final boolean isChangeValid;
  private Aspect aspect;
  private List<String> messages;

  public ProcessChangeResult(Aspect aspect, boolean isChangeValid, List<String> messages) {
    this.aspect = aspect;
    this.messages = messages;
    this.isChangeValid  = isChangeValid;
  }

  public List<String> getMessages() {
    return messages;
  }

  public Aspect getAspect() {
    return aspect;
  }

  public boolean getIsChangeValid(){
    return isChangeValid;
  }
}
