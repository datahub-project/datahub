package com.linkedin.metadata.kafka.elasticsearch;

import com.datahub.util.RecordUtils;
import com.linkedin.data.template.RecordTemplate;
import java.io.IOException;
import javax.annotation.Nullable;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

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
      XContentParser parser =
          XContentFactory.xContent(XContentType.JSON)
              .createParser(
                  NamedXContentRegistry.EMPTY,
                  DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                  jsonString);
      builder.copyCurrentStructure(parser);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return builder;
  }
}
