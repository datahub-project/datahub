package com.linkedin.metadata.search.elasticsearch.query.template;

/**
 * A vector rung cannot serve this search, and no ladder fallback may hide that: this process cannot
 * run it, it selects a profile this deployment cannot embed, or it failed with {@code onFailure}
 * unset or {@code fail}. It fails the search even from a parallel group, whose members' other
 * failures follow the group's {@code next}.
 */
public class SearchTemplateFailClosedException extends SearchTemplateException {
  public SearchTemplateFailClosedException(String message) {
    super(message);
  }

  public SearchTemplateFailClosedException(String message, Throwable cause) {
    super(message, cause);
  }
}
