package io.datahubproject.openapi.v1.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
public class TraceStatus {
  private boolean success;
  private TraceStorageStatus primaryStorage;
  private TraceStorageStatus searchStorage;
}
