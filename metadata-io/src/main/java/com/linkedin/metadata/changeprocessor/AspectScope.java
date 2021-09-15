package com.linkedin.metadata.changeprocessor;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface AspectScope {
  String[] aspectNames();
}
