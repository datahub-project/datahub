package io.datahubproject.openapi.v2.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Value;


@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BatchGetUrnResponse implements Serializable {
  List<GenericEntity> entities;
}
