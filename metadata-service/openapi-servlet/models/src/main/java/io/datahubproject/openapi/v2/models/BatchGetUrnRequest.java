package io.datahubproject.openapi.v2.models;

import java.io.Serializable;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;


@Value
@EqualsAndHashCode
public class BatchGetUrnRequest implements Serializable {
  List<String> urns;
  List<String> aspectNames;
  boolean withSystemMetadata;
}
