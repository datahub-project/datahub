package com.linkedin.metadata.models.annotation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * Simple object representation of the @Aspect annotation metadata.
 */
public class AspectAnnotation {

    private final String _name;
    private final Boolean _isKey;

    public AspectAnnotation(@Nonnull final String name,
                            @Nullable final Boolean isKey) {
        _name = name;
        _isKey = isKey != null && isKey;
    }

    public String getName() {
        return _name;
    }

    public Boolean isKey() {
        return _isKey;
    }

    public static AspectAnnotation fromSchemaProperty(@Nonnull final Object annotationObj) {
        if (Map.class.isAssignableFrom(annotationObj.getClass())) {
            Map map = (Map) annotationObj;
            final Object nameObj = map.get("name");
            final Object isKeyObj = map.get("isKey");
            if (nameObj == null || !String.class.isAssignableFrom(nameObj.getClass())) {
                throw new IllegalArgumentException("Failed to validate required @Aspect field 'name' field of type String");
            }
            if (isKeyObj != null && !Boolean.class.isAssignableFrom(isKeyObj.getClass())) {
                throw new IllegalArgumentException("Failed to validate required @Aspect field 'isKey' field of type Boolean");
            }
            return new AspectAnnotation((String) nameObj, (Boolean) isKeyObj);
        }
        throw new IllegalArgumentException("Failed to validate @Aspect annotation object: Invalid value type provided (Expected Map)");
    }
}
