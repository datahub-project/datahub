package io.datahubproject.openapi.v3.models;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

@Data
@Jacksonized
@Builder
public class GenericEntityScrollRequestBodyV3 {
    @Nullable private Set<String> entities;
    @Nullable private Set<String> aspects;
    @Nullable private Filter filter;
    @Nullable private List<SortCriterion> sortCriteria;
}
