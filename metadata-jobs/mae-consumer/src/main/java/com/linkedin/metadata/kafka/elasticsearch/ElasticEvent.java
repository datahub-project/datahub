package com.linkedin.metadata.kafka.elasticsearch;

import com.linkedin.events.metadata.ChangeType;
import lombok.Data;
import org.opensearch.core.xcontent.XContentBuilder;

@Data
public abstract class ElasticEvent {

  private String index;
  private String type;
  private String id;
  private ChangeType actionType;

  public XContentBuilder buildJson() {
    return null;
  }
}
