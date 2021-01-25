package com.linkedin.metadata;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.entity.BaseEntity;
import com.linkedin.metadata.relationship.BaseRelationship;
import com.linkedin.metadata.search.BaseDocument;
import java.util.Set;


public class ModelValidationConstants {

  private ModelValidationConstants() {
    // Util class
  }

  static final Set<Class<? extends RecordTemplate>> IGNORED_ENTITY_CLASSES = ImmutableSet.of(BaseEntity.class);

  static final Set<Class<? extends RecordTemplate>> IGNORED_RELATIONSHIP_CLASSES =
      ImmutableSet.of(BaseRelationship.class);

  static final Set<Class<? extends RecordTemplate>> IGNORED_DOCUMENT_CLASSES = ImmutableSet.of(BaseDocument.class);

  static final Set<Class<? extends UnionTemplate>> IGNORED_ASPECT_CLASSES = ImmutableSet.of();

  static final Set<Class<? extends RecordTemplate>> IGNORED_SNAPSHOT_CLASSES = ImmutableSet.of();

  static final Set<Class<? extends RecordTemplate>> IGNORED_DELTA_CLASSES = ImmutableSet.of();
}
