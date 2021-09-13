package com.linkedin.metadata.models;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;

import java.util.List;
import java.util.Map;

public interface EntitySpec {
    String getName();

    EntityAnnotation getEntityAnnotation();

    String getKeyAspectName();

    AspectSpec getKeyAspectSpec();

    List<AspectSpec> getAspectSpecs();

    Map<String, AspectSpec> getAspectSpecMap();

    Boolean hasAspect(String name);

    AspectSpec getAspectSpec(String name);

    RecordDataSchema getSnapshotSchema();

    TyperefDataSchema getAspectTyperefSchema();

    List<SearchableFieldSpec> getSearchableFieldSpecs();
}
