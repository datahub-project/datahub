package com.linkedin.metadata.changeprocessor;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


@Retention(RetentionPolicy.RUNTIME)
public @interface ChangeProcessorScope {
  // Must be in the form entity:aspect
  String[] entityAspectNames();

  ChangeProcessorType processorType();
}

