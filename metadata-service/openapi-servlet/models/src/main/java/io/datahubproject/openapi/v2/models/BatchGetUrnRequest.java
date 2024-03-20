package io.datahubproject.openapi.v2.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.Serializable;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode
@Builder
@JsonDeserialize(builder = BatchGetUrnRequest.BatchGetUrnRequestBuilder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BatchGetUrnRequest implements Serializable {
  List<String> urns;
  List<String> aspectNames;
  boolean withSystemMetadata;
}
