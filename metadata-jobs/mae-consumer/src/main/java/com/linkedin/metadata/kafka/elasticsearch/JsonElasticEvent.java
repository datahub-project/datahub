/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.elasticsearch;

import java.io.IOException;
import javax.annotation.Nullable;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class JsonElasticEvent extends ElasticEvent {
  private final String _document;

  public JsonElasticEvent(String document) {
    this._document = document;
  }

  @Override
  @Nullable
  public XContentBuilder buildJson() {
    XContentBuilder builder = null;
    try {
      builder = XContentFactory.jsonBuilder().prettyPrint();
      XContentParser parser =
          XContentType.JSON
              .xContent()
              .createParser(
                  NamedXContentRegistry.EMPTY,
                  DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                  _document);
      builder.copyCurrentStructure(parser);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return builder;
  }
}
