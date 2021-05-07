package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;

import javax.annotation.Nonnull;
import java.util.List;

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

    public DataSchema getPegasusSchema() {
        return _pegasusSchema;
    }
}
