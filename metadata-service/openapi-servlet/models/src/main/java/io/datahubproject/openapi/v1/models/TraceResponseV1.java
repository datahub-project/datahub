package io.datahubproject.openapi.v1.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.common.urn.Urn;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
public class TraceResponseV1 extends LinkedHashMap<Urn, Map<String, TraceStatus>> {
  public TraceResponseV1(Map<? extends Urn, ? extends Map<String, TraceStatus>> m) {
    super(m);
  }
}
