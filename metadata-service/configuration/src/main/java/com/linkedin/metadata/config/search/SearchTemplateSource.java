package com.linkedin.metadata.config.search;

/** Where a process reads search templates. */
public enum SearchTemplateSource {
  /** Stored aspect, then the shipped bootstrap file when that aspect is missing. */
  STORAGE,
  /** Shipped bootstrap file only. System update uses this for the whole process. */
  CLASSPATH
}
