package io.datahubproject.openapi.v3.models;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class GenericScrollResult {
    private String scrollId;
    private List<GenericEntity> entities;
}
