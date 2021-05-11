package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;

import javax.annotation.Nonnull;
import java.util.List;

public class SearchableFieldSpec {

    private final PathSpec _path;
    private final SearchableAnnotation _searchableAnnotation;
    private final DataSchema _pegasusSchema;

    public SearchableFieldSpec(@Nonnull final PathSpec path,
                               @Nonnull final DataSchema pegasusSchema,
                               @Nonnull final SearchableAnnotation searchableAnnotation) {
        _path = path;
        _pegasusSchema = pegasusSchema;
        _searchableAnnotation = searchableAnnotation;
    }

    public PathSpec getPath() {
        return _path;
    }

    public String getFieldName() {
        return _searchableAnnotation.getFieldName();
    }

    public boolean isDefaultAutocomplete() {
        return _searchableAnnotation.isDefaultAutocomplete();
    }

    public boolean addToFilters() {
        return _searchableAnnotation.isAddToFilters();
    }

    public List<SearchableAnnotation.IndexSetting> getIndexSettings() {
        return _searchableAnnotation.getIndexSettings();
    }

    public DataSchema getPegasusSchema() {
        return _pegasusSchema;
    }
}