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
