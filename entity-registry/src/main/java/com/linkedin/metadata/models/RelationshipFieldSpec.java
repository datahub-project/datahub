package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import lombok.NonNull;
import lombok.Value;

import java.util.List;

@Value
public class RelationshipFieldSpec implements FieldSpec {

    @NonNull PathSpec path;
    @NonNull RelationshipAnnotation relationshipAnnotation;
    @NonNull DataSchema pegasusSchema;

    public String getRelationshipName() {
        return relationshipAnnotation.getName();
    }

    public List<String> getValidDestinationTypes() {
        return relationshipAnnotation.getValidDestinationTypes();
    }

}
