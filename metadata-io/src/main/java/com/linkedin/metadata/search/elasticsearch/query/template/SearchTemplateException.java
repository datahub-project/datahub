package com.linkedin.metadata.search.elasticsearch.query.template;

/** A search template could not be selected, loaded, or rendered. The search fails. */
public class SearchTemplateException extends RuntimeException {
  public SearchTemplateException(String message) {
    super(message);
  }

  public SearchTemplateException(String message, Throwable cause) {
    super(message, cause);
  }
}
