package com.linkedin.metadata.models.annotation;

import lombok.Value;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Simple object representation of the @Relationship annotation metadata.
 */
@Value
public class RelationshipAnnotation {

    String name;
    List<String> validDestinationTypes;

    public static RelationshipAnnotation fromPegasusAnnotationObject(@Nonnull final Object annotationObj) {
        if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
            throw new IllegalArgumentException("Failed to validate Relationship annotation object: Invalid value type provided (Expected Map)");
        }

        Map map = (Map) annotationObj;
        final Optional<String> name = AnnotationUtils.getField(map, "name", String.class);
        if (!name.isPresent()) {
            throw new IllegalArgumentException("Failed to validate required Relationship field 'name' field of type String");
        }

        final Optional<List> entityTypesList = AnnotationUtils.getField(map, "entityTypes", List.class);
        if (!entityTypesList.isPresent() || entityTypesList.get().isEmpty()) {
            throw new IllegalArgumentException("Failed to validate required Relationship field 'entityTypes' field of type List<String>");
        }
        final List<String> entityTypes = new ArrayList<>(entityTypesList.get().size());
        for (Object entityTypeObj : entityTypesList.get()) {
            if (!String.class.isAssignableFrom(entityTypeObj.getClass())) {
                throw new IllegalArgumentException(
                        "Failed to validate Relationship field 'entityTypes' field of type List<String>: Invalid values provided (Expected String)");
            }
            entityTypes.add((String) entityTypeObj);
        }

        return new RelationshipAnnotation(name.get(), entityTypes);
    }

}