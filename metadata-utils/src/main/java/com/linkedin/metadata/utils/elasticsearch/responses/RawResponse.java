/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
