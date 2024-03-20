package io.datahubproject.openapi.v2.models;

import java.io.Serializable;
import java.util.List;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class BatchGetUrnResponse implements Serializable {
  private final List<GenericEntity> entities;
}
