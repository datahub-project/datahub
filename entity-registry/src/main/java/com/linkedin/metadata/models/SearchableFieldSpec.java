package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import lombok.NonNull;
import lombok.Value;

import java.util.List;

@Value
public class SearchableFieldSpec implements FieldSpec {

    @NonNull PathSpec path;
    @NonNull SearchableAnnotation searchableAnnotation;
    @NonNull DataSchema pegasusSchema;

    public String getFieldName() {
        return searchableAnnotation.getFieldName();
    }

    public boolean isDefaultAutocomplete() {
        return searchableAnnotation.isDefaultAutocomplete();
    }

    public boolean addToFilters() {
        return searchableAnnotation.isAddToFilters();
    }

    public List<SearchableAnnotation.IndexSetting> getIndexSettings() {
        return searchableAnnotation.getIndexSettings();
    }

}