package com.linkedin.metadata.aspect.plugins.validation;

public class AspectValidationException extends Exception {

  public AspectValidationException(String msg) {
    super(msg);
  }

  public AspectValidationException(String msg, Exception e) {
    super(msg, e);
  }
}
