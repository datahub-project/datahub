package com.linkedin.metadata.kafka.elasticsearch;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import javax.annotation.Nullable;

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
      XContentParser parser = XContentFactory.xContent(XContentType.JSON)
          .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, _document);
      builder.copyCurrentStructure(parser);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return builder;
  }
}
