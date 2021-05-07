package com.linkedin.experimental;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;

import javax.annotation.Nonnull;
import java.util.Map;

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

    public SearchableAnnotation getSearchableAnnotation() {
        return _searchableAnnotation;
    }

    public DataSchema getPegasusSchema() {
        return _pegasusSchema;
    }

    public static class SearchableAnnotation {

        private final IndexType _indexType;

        // TODO Expand.
        public enum IndexType {
            KEYWORD
        }

        public static SearchableAnnotation fromPegasusAnnotationObject(@Nonnull final Object annotationObj) {
            if (Map.class.isAssignableFrom(annotationObj.getClass())) {
                Map map = (Map) annotationObj;
                final Object indexTypeObj = map.get("indexType");
                if (indexTypeObj == null || !String.class.isAssignableFrom(indexTypeObj.getClass())) {
                    throw new IllegalArgumentException("Failed to validate required Searchable field 'indexType' field of type String");
                }
                try {
                    return new SearchableAnnotation(IndexType.valueOf((String) indexTypeObj));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(String.format(
                            "Failed to validate required Searchable field 'indexType'. Invalid indexType provided. Valid types are %s",
                            IndexType.values().toString()));
                }
            }
            throw new IllegalArgumentException("Failed to validate Searchable annotation object: Invalid value type provided (Expected Map)");
        }

        public SearchableAnnotation(final IndexType indexType) {
            _indexType = indexType;
        }

        public IndexType getIndexType() {
            return _indexType;
        }

    }
}