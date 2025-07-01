package io.datahubproject.openapi.v1.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.common.urn.Urn;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
public class TraceRequestV1 extends LinkedHashMap<Urn, List<String>> {}
