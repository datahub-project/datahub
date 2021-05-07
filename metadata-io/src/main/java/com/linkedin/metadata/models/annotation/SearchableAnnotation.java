package com.linkedin.metadata.models.annotation;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Simple object representation of the @Searchable annotation metadata.
 */
public class SearchableAnnotation {

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
