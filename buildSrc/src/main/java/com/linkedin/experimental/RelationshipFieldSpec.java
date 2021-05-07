package com.linkedin.experimental;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public class RelationshipFieldSpec {

    private final PathSpec _path;
    private final RelationshipAnnotation _relationshipAnnotation;
    private final DataSchema _pegasusSchema;

    public RelationshipFieldSpec(@Nonnull final PathSpec path,
                                 @Nonnull final DataSchema pegasusSchema,
                                 @Nonnull final RelationshipAnnotation relationshipAnnotation) {
        _path = path;
        _pegasusSchema = pegasusSchema;
        _relationshipAnnotation = relationshipAnnotation;
    }

    public PathSpec getPath() {
        return _path;
    }

    public String getRelationshipName() {
        return _relationshipAnnotation.getName();
    }

    public List<String> getValidDestinationTypes() {
        return _relationshipAnnotation.getValidDestinationTypes();
    }

    public RelationshipAnnotation getRelationshipAnnotation() {
        return _relationshipAnnotation;
    }

    public DataSchema getPegasusSchema() {
        return _pegasusSchema;
    }

    public static class RelationshipAnnotation {

        private final String _name;
        private final List<String> _validDestinationTypes;

        public static RelationshipAnnotation fromPegasusAnnotationObject(@Nonnull final Object annotationObj) {
            if (Map.class.isAssignableFrom(annotationObj.getClass())) {
                Map map = (Map) annotationObj;
                final Object nameObj = map.get("name");
                final Object entityTypesObj = map.get("entityTypes");
                if (nameObj == null || !String.class.isAssignableFrom(nameObj.getClass())) {
                    throw new IllegalArgumentException("Failed to validate required Relationship field 'name' field of type String");
                }
                if (entityTypesObj == null || !List.class.isAssignableFrom(entityTypesObj.getClass())) {
                    throw new IllegalArgumentException("Failed to validate required Relationship field 'entityTypes' field of type List<String>");
                }
                final String name = (String) nameObj;
                final List entityTypes = (List) entityTypesObj;
                for (Object entityTypeObj : entityTypes) {
                    if (!(String.class.isAssignableFrom(entityTypeObj.getClass()))) {
                        throw new IllegalArgumentException("Failed to validate Relationship field 'entityTypes' field of type List<String>: Invalid values provided (Expected String)");
                    }
                }
                return new RelationshipAnnotation(name, (List<String>) entityTypes);
            }
            throw new IllegalArgumentException("Failed to validate Relationship annotation object: Invalid value type provided (Expected Map)");
        }

        public RelationshipAnnotation(final String name,
                                      final List<String> validDestinationTypes) {
            _name = name;
            _validDestinationTypes = validDestinationTypes;
        }

        public String getName() {
            return _name;
        }

        public List<String> getValidDestinationTypes() {
            return _validDestinationTypes;
        }
    }
}
