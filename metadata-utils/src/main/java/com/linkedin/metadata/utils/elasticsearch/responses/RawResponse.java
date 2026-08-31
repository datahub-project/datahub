package com.linkedin.metadata.utils.elasticsearch.responses;

import javax.annotation.Nonnull;
import lombok.Data;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;

@Data
public class RawResponse {
  @Nonnull private final RequestLine requestLine;
  @Nonnull private final HttpHost host;
  @Nonnull private final HttpEntity entity;
  @Nonnull private final StatusLine statusLine;

  public RawResponse(
      RequestLine requestLine, HttpHost host, HttpEntity entity, StatusLine statusLine) {
    this.requestLine = requestLine;
    this.host = host;
    this.entity = entity;
    this.statusLine = statusLine;
  }
}
