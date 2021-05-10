package com.linkedin.metadata.models.annotation;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.Value;


/**
 * Simple object representation of the @Searchable annotation metadata.
 */
@RequiredArgsConstructor
public class SearchableAnnotation {

    private final String _fieldName;
    private final List<IndexSetting> _indexSettings;

    public enum IndexType {
        KEYWORD,
        BOOLEAN,
        BROWSE_PATH,
        DELIMITED,
        PATTERN,
        PARTIAL,
        PARTIAL_SHORT,
        PARTIAL_LONG,
        PARTIAL_PATTERN
    }

    @Value
    public static class IndexSetting {
        IndexType indexType;
        boolean queryByDefault;
        Optional<Double> boostScore;
    }

    private static 

    public static SearchableAnnotation fromPegasusAnnotationObject(@Nonnull final Object annotationObj) {
        if (Map.class.isAssignableFrom(annotationObj.getClass())) {
            Map map = (Map) annotationObj;
            final Object indexNameObj = map.get("indexName");
            if (indexNameObj == null || !String.class.isAssignableFrom(indexNameObj.getClass())) {
                throw new IllegalArgumentException("Failed to validate required Searchable field 'indexType' field of type String");
            }
            try {
                return new SearchableAnnotation((String) indexNameObj);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                        "Failed to validate required Searchable field 'indexType'. Invalid indexType provided. Valid types are %s",
                        IndexType.values().toString()));
            }
        }
        throw new IllegalArgumentException("Failed to validate Searchable annotation object: Invalid value type provided (Expected Map)");
    }

    public IndexType getIndexType() {
        return _indexType;
    }

}
