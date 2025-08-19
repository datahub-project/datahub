package io.datahubproject.openapi.v3.models;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericEntitySearchResultV3 {
    private String cursor;
    private List<GenericEntityV3> entities;
}