package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import javax.annotation.Nonnull;

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

    public SearchableAnnotation.IndexType getIndexType() {
        return _searchableAnnotation.getIndexType();
    }

    public DataSchema getPegasusSchema() {
        return _pegasusSchema;
    }
}