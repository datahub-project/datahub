package com.linkedin.metadata.search.elasticsearch.query.template;

/**
 * A similarity search whose query names no entity it can anchor on: the query is not an entity URN,
 * or that entity cannot anchor it, because the viewer may not use it, the default search filters or
 * the search's document visibility rule leave it out, its type has no embedding profile, or it has
 * no stored vectors. The request, not the service, is at fault, so callers can report it as a
 * client error. A profile that cannot be read or selected is a service fault and fails as
 * unavailable instead.
 */
public class SearchTemplateNoAnchorException extends SearchTemplateFailClosedException {
  public SearchTemplateNoAnchorException(String message) {
    super(message);
  }
}
