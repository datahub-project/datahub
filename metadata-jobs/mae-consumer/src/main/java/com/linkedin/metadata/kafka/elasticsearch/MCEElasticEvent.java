package com.linkedin.metadata.kafka.elasticsearch;

import com.linkedin.data.template.RecordTemplate;
import com.datahub.util.RecordUtils;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import javax.annotation.Nullable;


public class MCEElasticEvent extends ElasticEvent {

  private final RecordTemplate _doc;

  public MCEElasticEvent(RecordTemplate doc) {
    this._doc = doc;
  }

  @Override
  @Nullable
  public XContentBuilder buildJson() {
    XContentBuilder builder = null;
    try {
      String jsonString = RecordUtils.toJsonString(this._doc);
      builder = XContentFactory.jsonBuilder().prettyPrint();
      XContentParser parser = XContentFactory.xContent(XContentType.JSON)
          .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonString);
      builder.copyCurrentStructure(parser);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return builder;
  }
}
